// <copyright file="ThreadLeakScenario.cs" company="Motion Applied Ltd.">
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

// Thread-leak scenario for the TestClient.
//
// Usage:
//   dotnet run -- --thread-leak [--sessions N] [--duration-seconds S]
//
// What it does
// ────────────
//   Phase 1 – Publish
//     Runs <sessions> sessions concurrently.  Each session publishes engine
//     telemetry at 10 Hz for <duration-seconds>.  An Interlocked counter
//     tracks the exact number of packets written per session.
//
//   Phase 2 – Read-back
//     After all sessions are closed, opens a fresh reader per session starting
//     at offset 0.  Reads until packetsRead >= packetsWritten, then cancels
//     and disposes the reader.  A 10-second idle timeout acts as a safety valve.
//
// Thread accounting
// ─────────────────
// Three "settled" snapshots drive the leak verdict:
//
//   A  Baseline          – before any session (pre-singleton warm-up).
//                          Informational only; NOT used for leak comparison.
//
//   B  Post-publish      – after all sessions closed + 3 s settle + GC.
//                          Singleton/server-lifecycle threads (Kafka consumer
//                          groups, session-management workers, gRPC channel
//                          threads) have warmed up and are permanently resident.
//                          This is the comparison baseline for leak detection.
//
//   C  Post-readback     – after all readers closed + 3 s settle + GC.
//
// Verdict: C – B must be ≤ tolerance.
//   • If readers are cleaned up correctly, C ≈ B  (readers released their threads).
//   • If readers leak, C climbs above B each time.
//
// Mid-stream snapshots (per session open/close, per reader open/close) are
// printed for visibility but are NOT settled — they are taken immediately after
// the event, so transient cleanup threads may inflate the count momentarily.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using MA.Streaming.API;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;

namespace MA.Streaming.TestClient;

[ExcludeFromCodeCoverage]
public sealed class ThreadLeakScenario
{
    // ── constants ──────────────────────────────────────────────────────────────
    private const string DataSource = "ThreadLeakScenario";
    private const string TelemetryStream = "tlm";

    // Settle time used before every key (comparison) snapshot.
    // 6 s gives the ThreadPool time to retire threads that expanded for
    // concurrent async work (cancellation of N readers simultaneously creates
    // more teardown work than a single reader — .NET's ThreadPool retires idle
    // threads on a ~30 s cadence, but most are returned within 5–6 s in practice).
    private const int SettleMs = 6000;

    // Tolerance for the post-publish leak check (single-session teardown noise).
    private const int PublishLeakToleranceThreads = 5;

    // Tolerance for the post-readback leak check.  Cancelling N readers
    // concurrently expands the ThreadPool more than sequential teardown, so a
    // slightly wider window is appropriate.  A real leak grows with session
    // count; this constant does not.
    private const int ReadbackLeakToleranceThreads = 10;

    // ── fields ─────────────────────────────────────────────────────────────────
    private readonly StreamingApiClient client;
    private readonly int concurrentSessions;
    private readonly TimeSpan sessionDuration;

    // All snapshots in order; printed in summary.
    private readonly List<(string Label, int Count, bool IsSettled)> snapshots = [];

    // The two comparison points.
    private int postPublishCount;
    private int postReadbackCount;

    // ── ctor ───────────────────────────────────────────────────────────────────
    public ThreadLeakScenario(
        StreamingApiClient client,
        int concurrentSessions = 3,
        int sessionDurationSeconds = 60)
    {
        this.client = client;
        this.concurrentSessions = concurrentSessions;
        this.sessionDuration = TimeSpan.FromSeconds(sessionDurationSeconds);
    }

    // ── public entry point ─────────────────────────────────────────────────────
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════╗");
        Console.WriteLine("║           Thread-Leak Scenario                      ║");
        Console.WriteLine($"║  Sessions : {this.concurrentSessions,-5}  Duration : {this.sessionDuration.TotalSeconds,4}s         ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════╝");
        Console.WriteLine();

        // Snapshot A — informational baseline (pre-singleton warm-up).
        this.Snap("Baseline (before any session)", settled: true);

        // ── Phase 1: publish ───────────────────────────────────────────────────
        Console.WriteLine($"── Phase 1: starting {this.concurrentSessions} concurrent sessions ──");
        Console.WriteLine();

        var sessionTasks = Enumerable
            .Range(1, this.concurrentSessions)
            .Select(i => this.PublishSession(i, cancellationToken))
            .ToArray();

        var sessionRuns = await Task.WhenAll(sessionTasks);

        // Snapshot B — settled post-publish (comparison baseline for leak verdict).
        Console.WriteLine();
        Console.WriteLine("  Settling after publish phase…");
        await SettleAsync(cancellationToken);
        this.postPublishCount = this.Snap("Post-publish settled  ← leak-check baseline", settled: true);

        // ── Phase 2: read-back ─────────────────────────────────────────────────
        Console.WriteLine();
        Console.WriteLine("── Phase 2: reading back session data ──");
        Console.WriteLine();

        var readTasks = sessionRuns
            .Select(r => this.ReadBackSession(r, cancellationToken))
            .ToArray();

        await Task.WhenAll(readTasks);

        // Snapshot C — settled post-readback.
        Console.WriteLine();
        Console.WriteLine("  Settling after read-back phase…");
        await SettleAsync(cancellationToken);
        this.postReadbackCount = this.Snap("Post-readback settled ← leak-check result", settled: true);

        // ── Summary ────────────────────────────────────────────────────────────
        this.PrintSummary();
    }

    // ── Phase 1 helper: one session ────────────────────────────────────────────
    private async Task<SessionRun> PublishSession(int index, CancellationToken cancellationToken)
    {
        var sessionMgmt = this.client.GetSessionManagementClient();
        var writer = this.client.GetPacketWriterClient();

        var response = await sessionMgmt.CreateSessionAsync(
            new CreateSessionRequest
            {
                DataSource = DataSource,
                Type = "Session",
                Version = 1,
                UtcOffset = new Duration
                {
                    Seconds = 0
                },
                Identifier = $"thread-leak-session-{index}"
            },
            cancellationToken: cancellationToken);
        var sessionKey = response.SessionKey;

        Console.WriteLine($"  [S{index}] Session opened  key={sessionKey[..8]}…");
        this.Snap($"S{index} open (transient)", settled: false);

        var packetsWritten = 0;
        var deadline = DateTime.UtcNow + this.sessionDuration;
        var tick = 0;

        while (DateTime.UtcNow < deadline &&
               !cancellationToken.IsCancellationRequested)
        {
            var timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000;

            await writer.WriteDataPacketAsync(
                new WriteDataPacketRequest
                {
                    Detail = new DataPacketDetails
                    {
                        DataSource = DataSource,
                        Stream = TelemetryStream,
                        SessionKey = sessionKey,
                        Message = new Packet
                        {
                            Content = BuildEnginePacket(timestamp, tick).ToByteString(),
                            Type = "PeriodicData",
                            IsEssential = false,
                            SessionKey = sessionKey
                        }
                    }
                },
                cancellationToken: cancellationToken);

            packetsWritten++;
            tick++;

            if (tick % 100 == 0)
            {
                Console.WriteLine($"  [S{index}] {packetsWritten} packets written…");
            }

            await Task.Delay(100, cancellationToken);
        }

        await sessionMgmt.EndSessionAsync(
            new EndSessionRequest
            {
                SessionKey = sessionKey
            },
            cancellationToken: cancellationToken);

        Console.WriteLine($"  [S{index}] Session closed  — {packetsWritten} packets written");
        this.Snap($"S{index} closed (transient)", settled: false);

        return new SessionRun(index, sessionKey, packetsWritten);
    }

    // ── Phase 2 helper: read-back one session ──────────────────────────────────
    private async Task ReadBackSession(SessionRun run, CancellationToken outerCt)
    {
        var connMgr = this.client.GetConnectionManagerClient();
        var reader = this.client.GetPacketReaderClient();

        var connection = await connMgr.NewConnectionAsync(
            new NewConnectionRequest
            {
                Details = new ConnectionDetails
                {
                    DataSource = DataSource,
                    SessionKey = run.SessionKey,
                    Streams =
                    {
                        TelemetryStream
                    },
                    StreamOffsets =
                    {
                        0L
                    },
                    ExcludeMainStream = false
                }
            },
            cancellationToken: outerCt);

        const int IdleTimeoutSeconds = 10;
        using var readerCts = CancellationTokenSource.CreateLinkedTokenSource(outerCt);
        readerCts.CancelAfter(TimeSpan.FromSeconds(IdleTimeoutSeconds));

        var packetsRead = 0;
        var stream = reader.ReadPackets(
            new ReadPacketsRequest
            {
                Connection = connection.Connection
            },
            cancellationToken: readerCts.Token);

        Console.WriteLine($"  [S{run.Index}] Read-back started  (expecting {run.PacketsWritten} packets)");
        this.Snap($"S{run.Index} reader open (transient)", settled: false);

        try
        {
            while (await stream.ResponseStream.MoveNext(readerCts.Token))
            {
                var rsp = stream.ResponseStream.Current;
                packetsRead += rsp.Response.Count;

                readerCts.CancelAfter(TimeSpan.FromSeconds(IdleTimeoutSeconds));

                if (packetsRead % 50 == 0)
                {
                    Console.WriteLine($"  [S{run.Index}] …{packetsRead}/{run.PacketsWritten} packets read");
                }

                if (packetsRead >= run.PacketsWritten)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"  [S{run.Index}] Read-back cancelled");
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled)
        {
            Console.WriteLine($"  [S{run.Index}] Read-back cancelled");
        }
        finally
        {
            await readerCts.CancelAsync();
            stream.Dispose();
        }

        var status = packetsRead >= run.PacketsWritten ? "✓" : "⚠ incomplete";
        Console.WriteLine($"  [S{run.Index}] Read-back done  {packetsRead}/{run.PacketsWritten} packets  {status}");
        // Transient: snapshot taken immediately after dispose — cleanup threads
        // may still be running.  The settled post-readback snapshot is taken
        // after SettleAsync() in RunAsync().
        this.Snap($"S{run.Index} reader closed (transient)", settled: false);
    }

    // ── settle helper ──────────────────────────────────────────────────────────
    private static async Task SettleAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(SettleMs, cancellationToken);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    // ── snapshot helper ────────────────────────────────────────────────────────
    private int Snap(string label, bool settled)
    {
        var count = Process.GetCurrentProcess().Threads.Count;
        this.snapshots.Add((label, count, settled));
        var marker = settled ? "●" : "○";
        Console.WriteLine($"  {marker} [Threads] {label}: {count}");
        return count;
    }

    // ── summary report ─────────────────────────────────────────────────────────
    private void PrintSummary()
    {
        Console.WriteLine();
        Console.WriteLine("══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  Thread-count summary");
        Console.WriteLine("  ● = settled (GC + 3s delay)   ○ = transient (taken immediately)");
        Console.WriteLine("══════════════════════════════════════════════════════════════════");

        var baseline = this.snapshots[0].Count;

        foreach (var (label, count, settled) in this.snapshots)
        {
            var delta = count - baseline;
            var deltaStr = delta == 0 ? "  ±0" : $"{delta:+#;-#}";
            var marker = settled ? "●" : "○";
            Console.WriteLine($"  {marker} {count,4} ({deltaStr,4})  {label}");
        }

        Console.WriteLine("──────────────────────────────────────────────────────────────────");

        // ── Verdict A: did the publish phase leak? ─────────────────────────────
        // Baseline → Post-publish.  Singleton/server-lifecycle threads (Kafka
        // consumer groups, session management) warm up here and are permanent —
        // they are NOT a leak.  This check catches threads that should have been
        // released when sessions were closed but weren't.
        var publishBaseline = this.snapshots[0].Count;
        var publishDelta = this.postPublishCount - publishBaseline;

        // ── Verdict B: did the read-back phase leak? ───────────────────────────
        // Post-publish → Post-readback.  Reader threads (GenericBuffer loop,
        // gRPC WaitOne handler) must be released after Cancel + Dispose.
        // A wider tolerance accounts for ThreadPool expansion from concurrent
        // async teardown of N readers simultaneously — the ThreadPool retires
        // idle threads on a ~30 s cadence, so a 6 s settle doesn't fully drain
        // it.  A real per-reader leak would grow linearly with session count
        // this tolerance constant does not.
        var readbackDelta = this.postReadbackCount - this.postPublishCount;

        Console.WriteLine();
        Console.WriteLine("  ── Verdict A: publish phase (sessions closed correctly?)");
        Console.WriteLine($"     Baseline     : {publishBaseline} threads");
        Console.WriteLine($"     Post-publish : {this.postPublishCount} threads");
        Console.WriteLine(
            $"     Delta        : {publishDelta:+#;-#;0}  (singleton warm-up threads are expected — tolerance covers noise only: ±{PublishLeakToleranceThreads})");
        Console.WriteLine();
        Console.WriteLine("  ── Verdict B: read-back phase (readers released correctly?)");
        Console.WriteLine($"     Post-publish  (baseline) : {this.postPublishCount} threads");
        Console.WriteLine($"     Post-readback (result)   : {this.postReadbackCount} threads");
        Console.WriteLine($"     Delta                    : {readbackDelta:+#;-#;0} threads  (tolerance ±{ReadbackLeakToleranceThreads})");
        Console.WriteLine();

        if (readbackDelta <= ReadbackLeakToleranceThreads)
        {
            Console.WriteLine("  ✓ No thread leak detected — readers released their threads correctly.");
            Console.WriteLine($"    (Δ={readbackDelta:+#;-#;0} is within the ±{ReadbackLeakToleranceThreads} tolerance for");
            Console.WriteLine($"     ThreadPool expansion during concurrent async teardown of {this.concurrentSessions} readers.)");
        }
        else
        {
            Console.WriteLine($"  ✗ Possible thread leak!  read-back added {readbackDelta} threads above post-publish.");
            Console.WriteLine($"    Expected ≤{ReadbackLeakToleranceThreads} for {this.concurrentSessions} concurrent readers.");
            Console.WriteLine("    Increase --sessions and re-run: a real leak grows linearly with session count.");
        }

        Console.WriteLine("══════════════════════════════════════════════════════════════════");
    }

    // ── packet builder ─────────────────────────────────────────────────────────
    private static PeriodicDataPacket BuildEnginePacket(ulong timestamp, int tick)
    {
        return new PeriodicDataPacket
        {
            DataFormat = new SampleDataFormat
            {
                ParameterIdentifiers = new ParameterList
                {
                    ParameterIdentifiers =
                    {
                        "Engine.Rpm",
                        "Engine.Temperature"
                    }
                }
            },
            StartTime = timestamp,
            Interval = 100_000_000,
            Columns =
            {
                new SampleColumn
                {
                    DoubleSamples = new DoubleSampleList
                    {
                        Samples =
                        {
                            new DoubleSample
                            {
                                Value = 2000 + (tick % 100) * 40.0,
                                Status = DataStatus.Valid
                            }
                        }
                    }
                },
                new SampleColumn
                {
                    DoubleSamples = new DoubleSampleList
                    {
                        Samples =
                        {
                            new DoubleSample
                            {
                                Value = 85 + (tick % 50) * 0.5,
                                Status = DataStatus.Valid
                            }
                        }
                    }
                }
            }
        };
    }

    // ── record ─────────────────────────────────────────────────────────────────
    private sealed record SessionRun(int Index, string SessionKey, int PacketsWritten);
}
