// <copyright file="LiveSessionMonitor.cs" company="Motion Applied Ltd.">
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

// Live session monitor.
//
// On start-up: queries GetCurrentSessions so sessions already running when the
// monitor launches are picked up immediately (not just new ones going forward).
//
// Then listens for GetSessionStartNotification / GetSessionStopNotification.
//
// When a session is detected (existing or new):
//   – prints a dedicated "Session Detected" block immediately
//
// Every 5 s:
//   – prints a compact statistics table (packet-type counts per active session
//     + process thread count)
//
// When a session stops:
//   – prints a "Session Ended" block with final totals

using System.Collections.Concurrent;
using System.Diagnostics;

using Grpc.Core;

using MA.Streaming.API;
using MA.Streaming.Proto.Client.Local;

namespace MA.Streaming.TestClient;

public sealed class LiveSessionMonitor
{
    private const int StatsIntervalSeconds = 5;
    private const int Width = 60;

    private readonly StreamingApiClient client;
    private readonly string dataSource;

    private readonly ConcurrentDictionary<string, SessionReader> activeSessions = new();
    private readonly ConcurrentQueue<SessionReader> closedSessions = new();

    public LiveSessionMonitor(StreamingApiClient client, string dataSource)
    {
        this.client = client;
        this.dataSource = dataSource;
    }

    // -- entry point ------------------------------------------------------------
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine($"Monitoring  data-source = {this.dataSource}");
        Console.WriteLine($"Statistics printed every {StatsIntervalSeconds} s  |  Ctrl+C to stop");
        Console.WriteLine();

        var sessionMgmt = this.client.GetSessionManagementClient();

        // -- register notification streams BEFORE querying current sessions -----
        // This order avoids a race: if a session starts between the query and the
        // listener registering, we would miss it.  With the listener already open,
        // the notification arrives and we de-duplicate against activeSessions.
        var startStream = sessionMgmt.GetSessionStartNotification(
            new GetSessionStartNotificationRequest { DataSource = this.dataSource },
            cancellationToken: cancellationToken);

        var stopStream = sessionMgmt.GetSessionStopNotification(
            new GetSessionStopNotificationRequest { DataSource = this.dataSource },
            cancellationToken: cancellationToken);

        // -- seed with sessions already running ---------------------------------
        try
        {
            var current = sessionMgmt.GetCurrentSessions(
                new GetCurrentSessionsRequest { DataSource = this.dataSource });

            foreach (var sessionKey in current.SessionKeys)
            {
                this.AttachSession(sessionKey, "(already running)", sessionMgmt, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[!] Could not query current sessions: {ex.Message}");
        }

        // -- run all background tasks -------------------------------------------
        await Task.WhenAll(
            this.ListenForStartsAsync(startStream, sessionMgmt, cancellationToken),
            this.ListenForStopsAsync(stopStream, sessionMgmt, cancellationToken),
            this.PrintStatsLoopAsync(cancellationToken));

        startStream.Dispose();
        stopStream.Dispose();
    }

    // -- notification listeners -------------------------------------------------
    private async Task ListenForStartsAsync(
        AsyncServerStreamingCall<GetSessionStartNotificationResponse> stream,
        SessionManagementService.SessionManagementServiceClient sessionMgmt,
        CancellationToken ct)
    {
        try
        {
            while (await stream.ResponseStream.MoveNext(ct))
            {
                var n = stream.ResponseStream.Current;
                this.AttachSession(n.SessionKey, "(new)", sessionMgmt, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled) { }
    }

    private async Task ListenForStopsAsync(
        AsyncServerStreamingCall<GetSessionStopNotificationResponse> stream,
        SessionManagementService.SessionManagementServiceClient sessionMgmt,
        CancellationToken ct)
    {
        try
        {
            while (await stream.ResponseStream.MoveNext(ct))
            {
                var n = stream.ResponseStream.Current;
                if (!this.activeSessions.TryRemove(n.SessionKey, out var reader))
                {
                    continue;
                }

                await reader.StopAsync();
                this.closedSessions.Enqueue(reader);
                this.PrintSessionEnded(reader, sessionMgmt);
            }
        }
        catch (OperationCanceledException) { }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled) { }
    }

    // -- session attach (shared by seed + start-notification) ------------------
    private void AttachSession(
        string sessionKey,
        string origin,
        SessionManagementService.SessionManagementServiceClient sessionMgmt,
        CancellationToken ct)
    {
        // De-duplicate: if a session is both in GetCurrentSessions AND triggers a
        // start notification shortly after, only attach it once.
        if (!this.activeSessions.TryAdd(sessionKey, null!))
        {
            return;
        }

        var reader = new SessionReader(sessionKey, this.dataSource, this.client);
        this.activeSessions[sessionKey] = reader;
        reader.StartReading(ct);

        this.PrintSessionDetected(sessionKey, origin, sessionMgmt);
    }

    // -- immediate print: session detected -------------------------------------
    private void PrintSessionDetected(
        string sessionKey,
        string origin,
        SessionManagementService.SessionManagementServiceClient sessionMgmt)
    {
        GetSessionInfoResponse? info = null;
        try
        {
            info = sessionMgmt.GetSessionInfo(new GetSessionInfoRequest { SessionKey = sessionKey });
        }
        catch { /* info stays null — not fatal */ }

        var line = new string('-', Width);
        Console.WriteLine();
        Console.WriteLine($"+{line}+");
        Console.WriteLine($"¦  ? SESSION DETECTED {origin,-37}¦");
        Console.WriteLine($"+{line}¦");
        Console.WriteLine($"¦  Key        : {sessionKey,-45}¦");

        if (info?.Success == true)
        {
            Console.WriteLine($"¦  Identifier : {(string.IsNullOrEmpty(info.Identifier) ? "(none)" : info.Identifier),-45}¦");
            Console.WriteLine($"¦  Type       : {(string.IsNullOrEmpty(info.Type) ? "(none)" : info.Type),-45}¦");
            Console.WriteLine($"¦  Version    : {info.Version,-45}¦");
            Console.WriteLine($"¦  DataSource : {(string.IsNullOrEmpty(info.DataSource) ? this.dataSource : info.DataSource),-45}¦");

            foreach (var kv in info.Details)
            {
                var label = kv.Key.Length > 10 ? kv.Key[..10] : kv.Key;
                Console.WriteLine($"¦  {label,-11}: {kv.Value,-45}¦");
            }
        }
        else
        {
            Console.WriteLine($"¦  DataSource : {this.dataSource,-45}¦");
        }

        Console.WriteLine($"¦  Detected   : {DateTime.Now:yyyy-MM-dd HH:mm:ss}  {origin,-29}¦");
        Console.WriteLine($"+{line}+");
        Console.WriteLine($"  Reading packets… statistics update every {StatsIntervalSeconds} s");
        Console.WriteLine();
    }

    // -- immediate print: session ended ----------------------------------------
    private void PrintSessionEnded(
        SessionReader reader,
        SessionManagementService.SessionManagementServiceClient sessionMgmt)
    {
        var line = new string('-', Width);
        Console.WriteLine();
        Console.WriteLine($"+{line}+");
        Console.WriteLine($"¦  ? SESSION ENDED{new string(' ', Width - 16)}¦");
        Console.WriteLine($"+{line}¦");
        Console.WriteLine($"¦  Key      : {reader.SessionKey,-47}¦");
        Console.WriteLine($"¦  Ended    : {DateTime.Now:yyyy-MM-dd HH:mm:ss}{new string(' ', 28)}¦");
        Console.WriteLine($"+{line}¦");
        Console.WriteLine($"¦  Final packet counts:{new string(' ', Width - 21)}¦");

        foreach (var (type, count) in reader.TypeCounts.OrderByDescending(x => x.Value))
        {
            Console.WriteLine($"¦    {type,-24}  {count,10:N0} packets{new string(' ', Width - 44)}¦");
        }

        if (reader.TypeCounts.IsEmpty)
        {
            Console.WriteLine($"¦    (no packets received){new string(' ', Width - 25)}¦");
        }

        Console.WriteLine($"+{line}+");
        Console.WriteLine();
    }

    // -- periodic statistics ----------------------------------------------------
    private async Task PrintStatsLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(StatsIntervalSeconds), ct);
                this.PrintStats();
            }
        }
        catch (OperationCanceledException) { }
    }

    private void PrintStats()
    {
        var active = this.activeSessions.Values
            .Where(r => r != null)
            .ToArray();

        // Drain closed sessions — shown once then discarded.
        var closed = new List<SessionReader>();
        while (this.closedSessions.TryDequeue(out var r))
        {
            closed.Add(r);
        }

        if (active.Length == 0 && closed.Count == 0)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}]  No active sessions  |  threads: {Process.GetCurrentProcess().Threads.Count}");
            return;
        }

        var border = new string('-', Width);
        Console.WriteLine($"+-- Statistics @ {DateTime.Now:HH:mm:ss} {new string('-', Width - 22)}+");
        Console.WriteLine($"¦  Threads : {Process.GetCurrentProcess().Threads.Count,-48}¦");
        Console.WriteLine($"¦  Active  : {active.Length,-48}¦");

        foreach (var r in active)
        {
            PrintSessionRow(r, "live");
        }

        foreach (var r in closed)
        {
            PrintSessionRow(r, "stopped");
        }

        Console.WriteLine($"+{border}+");
        Console.WriteLine();

        void PrintSessionRow(SessionReader r, string status)
        {
            Console.WriteLine($"¦{border}¦");
            var header = $"  {r.SessionKey[..Math.Min(8, r.SessionKey.Length)]}…  [{status}]";
            Console.WriteLine($"¦{header,-Width}¦");

            var counts = r.TypeCounts.OrderByDescending(x => x.Value).ToArray();
            if (counts.Length == 0)
            {
                Console.WriteLine($"¦  {"(waiting for packets...)",-Width - 2}  ¦");
            }
            else
            {
                foreach (var (type, count) in counts)
                {
                    var row = $"    {type,-24}  {count,10:N0} packets";
                    Console.WriteLine($"¦{row,-Width}¦");
                }
            }
        }
    }

    // -- inner: per-session packet reader --------------------------------------
    private sealed class SessionReader
    {
        private readonly StreamingApiClient client;
        private CancellationTokenSource? readerCts;
        private Task readerTask = Task.CompletedTask;

        public string SessionKey { get; }
        public string DataSource { get; }
        public ConcurrentDictionary<string, long> TypeCounts { get; } = new();

        public SessionReader(string sessionKey, string dataSource, StreamingApiClient client)
        {
            this.SessionKey = sessionKey;
            this.DataSource = dataSource;
            this.client = client;
        }

        public void StartReading(CancellationToken outerCt)
        {
            this.readerCts = CancellationTokenSource.CreateLinkedTokenSource(outerCt);
            this.readerTask = Task.Run(() => this.ReadLoopAsync(this.readerCts.Token));
        }

        public async Task StopAsync()
        {
            if (this.readerCts is null)
            {
                return;
            }

            await this.readerCts.CancelAsync();
            try
            {
                await this.readerTask.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (TimeoutException)
            {
                Console.WriteLine($"  [!] Reader for {this.SessionKey[..8]}… did not stop within 5 s");
            }
        }

        private async Task ReadLoopAsync(CancellationToken ct)
        {
            var connMgr = this.client.GetConnectionManagerClient();
            var packetReader = this.client.GetPacketReaderClient();
            AsyncServerStreamingCall<ReadPacketsResponse>? stream = null;

            try
            {
                // Omitting Streams ? server returns ALL streams for this session.
                // Omitting StreamOffsets ? each stream starts from offset 0 (Earliest),
                // so the monitor counts every packet from session start.
                // MainOffset default 0 = Earliest for the main stream too.
                var connection = await connMgr.NewConnectionAsync(
                    new NewConnectionRequest
                    {
                        Details = new ConnectionDetails
                        {
                            DataSource = this.DataSource,
                            SessionKey = this.SessionKey,
                            ExcludeMainStream = false
                            // Streams intentionally omitted ? reads all streams
                        }
                    },
                    cancellationToken: ct);

                stream = packetReader.ReadPackets(
                    new ReadPacketsRequest { Connection = connection.Connection },
                    cancellationToken: ct);

                while (await stream.ResponseStream.MoveNext(ct))
                {
                    foreach (var data in stream.ResponseStream.Current.Response)
                    {
                        var type = string.IsNullOrWhiteSpace(data.Packet.Type)
                            ? "(unknown)"
                            : data.Packet.Type;

                        this.TypeCounts.AddOrUpdate(type, 1L, (_, c) => c + 1L);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled) { }
            catch (Exception ex)
            {
                Console.WriteLine($"  [!] Reader error for {this.SessionKey[..8]}…: {ex.Message}");
            }
            finally
            {
                stream?.Dispose();
            }
        }
    }
}
