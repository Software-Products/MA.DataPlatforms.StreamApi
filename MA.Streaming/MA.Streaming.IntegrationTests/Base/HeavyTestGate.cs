// <copyright file="HeavyTestGate.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.IntegrationTests.Base;

/// <summary>
/// A process-wide gate that guarantees the heavyweight (1,000,000-message) batch
/// tests never run at the same time. All other tests stay fully parallel; only the
/// heavy tests serialise against one another so they don't overwhelm the brokers
/// when the plain and SASL collections execute concurrently.
/// </summary>
internal static class HeavyTestGate
{
    internal static readonly SemaphoreSlim Semaphore = new SemaphoreSlim(1, 1);
}
