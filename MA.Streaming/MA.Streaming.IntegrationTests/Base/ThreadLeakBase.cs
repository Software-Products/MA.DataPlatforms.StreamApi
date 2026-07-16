// <copyright file="ThreadLeakBase.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests.Base;

public abstract class ThreadLeakBase : IDisposable
{
    private const string TestStream = "threadLeakStream";
    private const int SessionCycles = 10;
    private const int PacketsPerSession = 10;
    private const int PostCloseSettleMs = 3000;
    private const int LeakToleranceThreads = 8;
    private const int WarmUpCycles = 2;

    protected abstract string BrokerUrl { get; }
    protected abstract string DataSource { get; }
    protected abstract string Domain { get; }

    protected virtual IKafkaSecurityConfigurationProvider CreateSecurityProvider() =>
        new EmptyKafkaSecurityConfigProvider();

    private readonly StreamingApiClient streamingApiClient;
    private readonly ITestOutputHelper output;

    protected ThreadLeakBase(ITestOutputHelper output)
    {
        this.output = output;
        var security = this.CreateSecurityProvider();
        var config = new StreamingApiConfiguration(
            StreamCreationStrategy.TopicBased,
            this.BrokerUrl,
            [],
            integrateDataFormatManagement: false,
            integrateSessionManagement: true,
            batchingResponses: false,
            domain: this.Domain,
            streamApiPort: ApiPortCreator.CreateNew());

        this.streamingApiClient = StreamingApiClientFactory.Create(
            config,
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(security),
            new LoggingDirectoryProvider(string.Empty),
            kafkaSecurityConfigProvider: security);

        this.streamingApiClient.Initialise();
        Thread.Sleep(1000);
    }

    public void Dispose() => this.streamingApiClient.Shutdown();

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
            new GetSessionStartNotificationRequest { DataSource = this.DataSource },
            cancellationToken: listenerCts.Token);
        var stopNotificationStream = sessionMgmt.GetSessionStopNotification(
            new GetSessionStopNotificationRequest { DataSource = this.DataSource },
            cancellationToken: listenerCts.Token);

        var startListenerTask = RunStartListener(startNotificationStream, startQueue, pendingStops, connectionMgr, packetReader, listenerCts.Token);
        var stopListenerTask = RunStopListener(stopNotificationStream, pendingStops, listenerCts.Token);

        await Task.Delay(500);
        var preSessionBaseline = Process.GetCurrentProcess().Threads.Count;
        this.output.WriteLine($"Pre-session baseline (listeners open, no session): {preSessionBaseline} threads");

        var postCloseThreadCounts = await RunSessionCycles(sessionMgmt, packetWriter, startQueue);

        await CancelListeners(listenerCts, startListenerTask, stopListenerTask);
        startNotificationStream.Dispose();
        stopNotificationStream.Dispose();
        await SettleThreads();

        var postListenerCount = Process.GetCurrentProcess().Threads.Count;
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
        return Task.Run(async () =>
        {
            try
            {
                while (await stream.ResponseStream.MoveNext(token))
                {
                    var notification = stream.ResponseStream.Current;
                    if (!startQueue.TryDequeue(out var state)) continue;
                    var connection = await connectionMgr.NewConnectionAsync(new NewConnectionRequest { Details = new ConnectionDetails { DataSource = notification.DataSource, SessionKey = notification.SessionKey, Streams = { TestStream }, StreamOffsets = { 0L }, ExcludeMainStream = false } }, cancellationToken: token);
                    state.ReaderCts = new CancellationTokenSource();
                    state.ReadStream = packetReader.ReadPackets(new ReadPacketsRequest { Connection = connection.Connection }, cancellationToken: state.ReaderCts.Token);
                    state.ReaderTask = Task.Run(async () => await ConsumePacketStream(state), state.ReaderCts.Token);
                    pendingStops[notification.SessionKey] = state;
                    state.ReaderOpenedSignal.TrySetResult(true);
                }
            }
            catch (OperationCanceledException) { }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled) { }
        }, token);
    }

    private static async Task ConsumePacketStream(SessionCycleState state)
    {
        try { while (await state.ReadStream.ResponseStream.MoveNext(state.ReaderCts.Token)) { } }
        catch (OperationCanceledException) { }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled) { }
    }

    private Task RunStopListener(AsyncServerStreamingCall<GetSessionStopNotificationResponse> stream, ConcurrentDictionary<string, SessionCycleState> pendingStops, CancellationToken token)
    {
        return Task.Run(async () =>
        {
            try
            {
                while (await stream.ResponseStream.MoveNext(token))
                {
                    var notification = stream.ResponseStream.Current;
                    if (!pendingStops.TryRemove(notification.SessionKey, out var state)) continue;
                    await state.ReaderCts.CancelAsync();
                    try { await state.ReaderTask.WaitAsync(TimeSpan.FromSeconds(5)); } catch (TimeoutException) { this.output.WriteLine($"WARNING: reader task for {notification.SessionKey} did not finish within 5 s"); }
                    state.ReadStream.Dispose();
                    state.ReaderClosedSignal.TrySetResult(true);
                }
            }
            catch (OperationCanceledException) { }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled) { }
        }, token);
    }

    private async Task<List<int>> RunSessionCycles(SessionManagementService.SessionManagementServiceClient sessionMgmt, PacketWriterService.PacketWriterServiceClient packetWriter, ConcurrentQueue<SessionCycleState> startQueue)
    {
        var postCloseThreadCounts = new List<int>();
        for (var cycle = 1; cycle <= SessionCycles; cycle++)
        {
            var state = new SessionCycleState(); startQueue.Enqueue(state);
            var createResponse = await sessionMgmt.CreateSessionAsync(new CreateSessionRequest { DataSource = this.DataSource, Type = "Session", Version = 1 });
            var sessionKey = createResponse.SessionKey;
            await state.ReaderOpenedSignal.Task.WaitAsync(TimeSpan.FromSeconds(15));
            await PublishPackets(packetWriter, sessionKey, cycle);
            await sessionMgmt.EndSessionAsync(new EndSessionRequest { SessionKey = sessionKey });
            await state.ReaderClosedSignal.Task.WaitAsync(TimeSpan.FromSeconds(15));
            await SettleThreads();
            var postCloseCount = Process.GetCurrentProcess().Threads.Count;
            postCloseThreadCounts.Add(postCloseCount);
            this.output.WriteLine($"Cycle {cycle,2}/{SessionCycles}: post-close threads = {postCloseCount}");
        }
        return postCloseThreadCounts;
    }

    private async Task PublishPackets(PacketWriterService.PacketWriterServiceClient packetWriter, string sessionKey, int cycle)
    {
        for (var i = 0; i < PacketsPerSession; i++)
        {
            await packetWriter.WriteDataPacketAsync(new WriteDataPacketRequest { Detail = new DataPacketDetails { DataSource = this.DataSource, Stream = TestStream, SessionKey = sessionKey, Message = new Packet { Content = ByteString.CopyFrom([(byte)i, (byte)cycle, 0xFF]), Type = "ThreadLeakTest", IsEssential = false, SessionKey = sessionKey } } });
            await Task.Delay(10);
        }
    }

    private static async Task CancelListeners(CancellationTokenSource listenerCts, Task startTask, Task stopTask)
    {
        await listenerCts.CancelAsync();
        await Task.WhenAll(startTask.WaitAsync(TimeSpan.FromSeconds(5)), stopTask.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    private static async Task SettleThreads() => await Task.Delay(PostCloseSettleMs);

    private void AssertNoThreadLeak(List<int> postCloseThreadCounts, int postListenerCount)
    {
        var warmUpCount = Math.Min(WarmUpCycles, postCloseThreadCounts.Count);
        var settledBaseline = postCloseThreadCounts.Take(warmUpCount).Max();
        this.output.WriteLine($"Settled baseline (max of first {warmUpCount} warm-up cycles): {settledBaseline} threads");
        this.output.WriteLine($"Leak tolerance: ±{LeakToleranceThreads} threads");
        for (var i = warmUpCount; i < postCloseThreadCounts.Count; i++)
        {
            var count = postCloseThreadCounts[i];
            var delta = count - settledBaseline;
            this.output.WriteLine($"  Cycle {i + 1,2}: {count} threads  (? {delta:+#;-#;0} vs baseline)");
            count.Should().BeLessOrEqualTo(settledBaseline + LeakToleranceThreads, because: $"cycle {i + 1} post-close thread count ({count}) must not exceed the settled baseline ({settledBaseline}) by more than {LeakToleranceThreads}.");
        }
        postListenerCount.Should().BeLessOrEqualTo(settledBaseline + LeakToleranceThreads, because: $"after cancelling notification streams, server-side handler threads must be released.");
    }

    private sealed class SessionCycleState
    {
        public TaskCompletionSource<bool> ReaderOpenedSignal { get; } = new();
        public TaskCompletionSource<bool> ReaderClosedSignal { get; } = new();
        public CancellationTokenSource ReaderCts { get; set; } = new();
        public AsyncServerStreamingCall<ReadPacketsResponse> ReadStream { get; set; } = null!;
        public Task ReaderTask { get; set; } = Task.CompletedTask;
    }
}
