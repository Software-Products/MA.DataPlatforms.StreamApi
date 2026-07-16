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
// -----------------
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
public class ThreadLeakShould : ThreadLeakBase
{
    public ThreadLeakShould(ITestOutputHelper output) : base(output) { }

    protected override string BrokerUrl => "localhost:9098";
    protected override string DataSource => "ThreadLeakDataSource";
    protected override string Domain => "ThreadLeakDomain";
}
