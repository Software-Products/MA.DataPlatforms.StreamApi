// <copyright file="ThreadLeakShould.cs" company="Motion Applied Ltd.">
//
// Copyright 2025 Motion Applied Ltd
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

// Reproduces the thread-leak scenario:
//
//   1. One always-running task listens on GetSessionStartNotification.
//      On each notification it creates a connection and opens a ReadPackets
//      stream, then starts a MoveNext loop – exactly as described in the report.
//
//   2. One always-running task listens on GetSessionStopNotification.
//      On each notification it cancels the reader CTS, awaits the MoveNext
//      task and disposes the stream – again exactly as described.
//
//   3. The test itself drives the cycle by calling CreateSession / EndSession,
//      which triggers the notifications (simulating what BridgeService does).
//
// Thread accounting
// ─────────────────
// While a session is open the following extra threads are legitimate:
//   • GetSessionStartNotification server handler (always running – 1 total)
//   • GetSessionStopNotification  server handler (always running – 1 total)
//   • GenericBuffer Task.Run loop          (per open reader)
//   • gRPC handler blocked on WaitOne()   (per open reader)
//
// After the session closes those per-reader threads must be released.
// The always-running notification-listener threads are expected to persist
// for the lifetime of the test and are absorbed into the settled baseline.
//
// The assertion compares post-close thread counts cycle-to-cycle.
// Cycle 1 establishes the settled baseline (includes warm-up).
// Cycles 2-N must stay within LeakToleranceThreads of that baseline.

using System.Collections.Concurrent;
using System.Diagnostics;

using FluentAssertions;

using Google.Protobuf;

using Grpc.Core;

using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.IntegrationTests.Base;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class ThreadLeakShould : IDisposable
{
    private const string BrokerUrl = "localhost:9098";
    private const string DataSource = "ThreadLeakDataSource";
    private const string TestStream = "threadLeakStream";
    private const string Domain = "ThreadLeakDomain";
    private const int SessionCycles = 10;
    private const int PacketsPerSession = 10;
    // Allow ThreadPool and server-side cleanup to settle before counting threads.
    private const int PostCloseSettleMs = 3000;
    // Tolerance for GC / ThreadPool scheduling variance.
    // CI agents run many xUnit collections in parallel, pushing the process-wide
    // thread count into the hundreds before this test even starts.  At that scale
    // the ThreadPool and Kafka consumer-group rebalancing introduce more noise
    // than in an isolated local run, so a wider tolerance is appropriate.
    // A real per-session leak grows by 10–20 threads per cycle; ±8 catches noise
    // without masking actual leaks.
    private const int LeakToleranceThreads = 8;
    // How many leading cycles to treat as warm-up before locking in the baseline.
    // Cycle 1 absorbs Kafka consumer-group bootstrap; cycle 2 absorbs any second
    // wave of ThreadPool expansion.  The baseline is taken from the highest count
    // seen in the warm-up window so subsequent cycles are compared fairly.
    private const int WarmUpCycles = 2;

    private readonly StreamingApiClient streamingApiClient;
    private readonly ITestOutputHelper output;

    public ThreadLeakShould(ITestOutputHelper output)
    {
        this.output = output;

        var config = new StreamingApiConfiguration(
            StreamCreationStrategy.TopicBased,
            BrokerUrl,
            [],
            integrateDataFormatManagement: false,
            integrateSessionManagement: true,
            batchingResponses: false,
            domain: Domain,
            streamApiPort: ApiPortCreator.CreateNew());

        this.streamingApiClient = StreamingApiClientFactory.Create(
            config,
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(),
            new LoggingDirectoryProvider(string.Empty));

        this.streamingApiClient.Initialise();
        Task.Delay(1000).Wait();
    }

    public void Dispose()
    {
        this.streamingApiClient.Shutdown();
    }

    [Fact]
    public async Task Not_Leak_Threads_Across_Session_Cycles()
    {
        var sessionMgmt = this.streamingApiClient.GetSessionManagementClient();
        var connectionMgr = this.streamingApiClient.GetConnectionManagerClient();
        var packetWriter = this.streamingApiClient.GetPacketWriterClient();
        var packetReader = this.streamingApiClient.GetPacketReaderClient();

        var startQueue = new ConcurrentQueue<SessionCycleState>();
        var pendingStops = new ConcurrentDictionary<string, SessionCycleState>();
        using var listenerCts = new CancellationTokenSource();

        var startNotificationStream = sessionMgmt.GetSessionStartNotification(
            new GetSessionStartNotificationRequest { DataSource = DataSource },
            cancellationToken: listenerCts.Token);

        var stopNotificationStream = sessionMgmt.GetSessionStopNotification(
            new GetSessionStopNotificationRequest { DataSource = DataSource },
            cancellationToken: listenerCts.Token);

        var startListenerTask = RunStartListener(
            startNotificationStream, startQueue, pendingStops, connectionMgr, packetReader, listenerCts.Token);

        var stopListenerTask = RunStopListener(
            stopNotificationStream, pendingStops, listenerCts.Token);

        await Task.Delay(500);
        var preSessionBaseline = Process.GetCurrentProcess().Threads.Count;
        this.output.WriteLine($"Pre-session baseline (listeners open, no session): {preSessionBaseline} threads");
        this.output.WriteLine(string.Empty);

        var postCloseThreadCounts = await RunSessionCycles(sessionMgmt, packetWriter, startQueue);

        await CancelListeners(listenerCts, startListenerTask, stopListenerTask);

        startNotificationStream.Dispose();
        stopNotificationStream.Dispose();

        await SettleThreads();

        var postListenerCount = Process.GetCurrentProcess().Threads.Count;
        this.output.WriteLine(string.Empty);
        this.output.WriteLine($"Post-listener-cancel threads: {postListenerCount}");

        AssertNoThreadLeak(postCloseThreadCounts, postListenerCount);
    }

    private Task RunStartListener(
        AsyncServerStreamingCall<GetSessionStartNotificationResponse> stream,
        ConcurrentQueue<SessionCycleState> startQueue,
        ConcurrentDictionary<string, SessionCycleState> pendingStops,
        ConnectionManagerService.ConnectionManagerServiceClient connectionMgr,
        PacketReaderService.PacketReaderServiceClient packetReader,
        CancellationToken token)
    {
        return Task.Run(
            async () =>
            {
                try
                {
                    while (await stream.ResponseStream.MoveNext(token))
                    {
                        var notification = stream.ResponseStream.Current;

                        if (!startQueue.TryDequeue(out var state))
                        {
                            continue;
                        }

                        var connection = await connectionMgr.NewConnectionAsync(
                            new NewConnectionRequest
                            {
                                Details = new ConnectionDetails
                                {
                                    DataSource = notification.DataSource,
                                    SessionKey = notification.SessionKey,
                                    Streams = { TestStream },
                                    StreamOffsets = { 0L },
                                    ExcludeMainStream = false
                                }
                            },
                            cancellationToken: token);

                        state.ReaderCts = new CancellationTokenSource();
                        state.ReadStream = packetReader.ReadPackets(
                            new ReadPacketsRequest { Connection = connection.Connection },
                            cancellationToken: state.ReaderCts.Token);

                        state.ReaderTask = Task.Run(
                            async () => await ConsumePacketStream(state),
                            state.ReaderCts.Token);

                        pendingStops[notification.SessionKey] = state;
                        state.ReaderOpenedSignal.TrySetResult(true);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected when listenerCts is cancelled at test teardown.
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled)
                {
                    // Expected when listenerCts is cancelled at test teardown.
                }
            },
            token);
    }

    private static async Task ConsumePacketStream(SessionCycleState state)
    {
        try
        {
            while (await state.ReadStream.ResponseStream.MoveNext(state.ReaderCts.Token))
            {
                // Intentionally empty – simulates customer code that only consumes.
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when reader is cancelled on session stop.
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled)
        {
            // Expected when reader is cancelled on session stop.
        }
    }

    private Task RunStopListener(
        AsyncServerStreamingCall<GetSessionStopNotificationResponse> stream,
        ConcurrentDictionary<string, SessionCycleState> pendingStops,
        CancellationToken token)
    {
        return Task.Run(
            async () =>
            {
                try
                {
                    while (await stream.ResponseStream.MoveNext(token))
                    {
                        var notification = stream.ResponseStream.Current;
                        if (!pendingStops.TryRemove(notification.SessionKey, out var state))
                        {
                            continue;
                        }

                        await state.ReaderCts.CancelAsync();

                        try
                        {
                            await state.ReaderTask.WaitAsync(TimeSpan.FromSeconds(5));
                        }
                        catch (TimeoutException)
                        {
                            this.output.WriteLine(
                                $"WARNING: reader task for session {notification.SessionKey} " +
                                "did not finish within 5 s after stop notification");
                        }

                        state.ReadStream.Dispose();
                        state.ReaderClosedSignal.TrySetResult(true);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected when listenerCts is cancelled at test teardown.
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled)
                {
                    // Expected when listenerCts is cancelled at test teardown.
                }
            },
            token);
    }

    private async Task<List<int>> RunSessionCycles(
        SessionManagementService.SessionManagementServiceClient sessionMgmt,
        PacketWriterService.PacketWriterServiceClient packetWriter,
        ConcurrentQueue<SessionCycleState> startQueue)
    {
        var postCloseThreadCounts = new List<int>();

        for (var cycle = 1; cycle <= SessionCycles; cycle++)
        {
            var state = new SessionCycleState();
            startQueue.Enqueue(state);

            var createResponse = await sessionMgmt.CreateSessionAsync(
                new CreateSessionRequest
                {
                    DataSource = DataSource,
                    Type = "Session",
                    Version = 1
                });

            var sessionKey = createResponse.SessionKey;
            await state.ReaderOpenedSignal.Task.WaitAsync(TimeSpan.FromSeconds(15));

            await PublishPackets(packetWriter, sessionKey, cycle);

            await sessionMgmt.EndSessionAsync(new EndSessionRequest { SessionKey = sessionKey });
            await state.ReaderClosedSignal.Task.WaitAsync(TimeSpan.FromSeconds(15));

            await SettleThreads();

            var postCloseCount = Process.GetCurrentProcess().Threads.Count;
            postCloseThreadCounts.Add(postCloseCount);

            this.output.WriteLine(
                $"Cycle {cycle,2}/{SessionCycles}: post-close threads = {postCloseCount}");
        }

        return postCloseThreadCounts;
    }

    private async Task PublishPackets(
        PacketWriterService.PacketWriterServiceClient packetWriter,
        string sessionKey,
        int cycle)
    {
        for (var i = 0; i < PacketsPerSession; i++)
        {
            await packetWriter.WriteDataPacketAsync(
                new WriteDataPacketRequest
                {
                    Detail = new DataPacketDetails
                    {
                        DataSource = DataSource,
                        Stream = TestStream,
                        SessionKey = sessionKey,
                        Message = new Packet
                        {
                            Content = ByteString.CopyFrom([(byte)i, (byte)cycle, 0xFF]),
                            Type = "ThreadLeakTest",
                            IsEssential = false,
                            SessionKey = sessionKey
                        }
                    }
                });

            await Task.Delay(10);
        }
    }

    private static async Task CancelListeners(
        CancellationTokenSource listenerCts,
        Task startListenerTask,
        Task stopListenerTask)
    {
        await listenerCts.CancelAsync();
        await Task.WhenAll(
            startListenerTask.WaitAsync(TimeSpan.FromSeconds(5)),
            stopListenerTask.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    private static async Task SettleThreads()
    {
        await Task.Delay(PostCloseSettleMs);
    }

    private void AssertNoThreadLeak(List<int> postCloseThreadCounts, int postListenerCount)
    {
        // Use the highest count seen across the warm-up window as the baseline.
        // This accounts for multi-cycle warm-up (Kafka rebalance, ThreadPool
        // expansion) without letting a genuine early leak inflate the baseline.
        var warmUpCount = Math.Min(WarmUpCycles, postCloseThreadCounts.Count);
        var settledBaseline = postCloseThreadCounts.Take(warmUpCount).Max();

        this.output.WriteLine(string.Empty);
        this.output.WriteLine(
            $"Settled baseline (max of first {warmUpCount} warm-up cycles): {settledBaseline} threads");
        this.output.WriteLine($"Leak tolerance: ±{LeakToleranceThreads} threads");
        this.output.WriteLine(string.Empty);

        for (var i = warmUpCount; i < postCloseThreadCounts.Count; i++)
        {
            var count = postCloseThreadCounts[i];
            var delta = count - settledBaseline;
            this.output.WriteLine(
                $"  Cycle {i + 1,2}: {count} threads  (Δ {delta:+#;-#;0} vs baseline)");

            count.Should().BeLessOrEqualTo(
                settledBaseline + LeakToleranceThreads,
                because: $"cycle {i + 1} post-close thread count ({count}) must not exceed " +
                          $"the settled baseline ({settledBaseline}) by more than {LeakToleranceThreads}. " +
                          $"A climbing count means per-session threads are not released.");
        }

        postListenerCount.Should().BeLessOrEqualTo(
            settledBaseline + LeakToleranceThreads,
            because: $"after cancelling notification streams, server-side handler threads " +
                      $"must be released. Post-cancel count ({postListenerCount}) must not exceed " +
                      $"the settled baseline ({settledBaseline}).");
    }

    // ────────────────────────────────────────────────────────────────────────

    private sealed class SessionCycleState
    {
        public TaskCompletionSource<bool> ReaderOpenedSignal { get; } = new();
        public TaskCompletionSource<bool> ReaderClosedSignal { get; } = new();
        public CancellationTokenSource ReaderCts { get; set; } = new();
        public AsyncServerStreamingCall<ReadPacketsResponse> ReadStream { get; set; } = null!;
        public Task ReaderTask { get; set; } = Task.CompletedTask;
    }
}
