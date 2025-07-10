// <copyright file="KafkaTestsCleanUpFixture.cs" company="McLaren Applied Ltd.">
//
// Copyright 2024 McLaren Applied Ltd
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

using System.Diagnostics.CodeAnalysis;

using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.Proto.Client.Local;

namespace MA.Streaming.IntegrationTests.Base;

[ExcludeFromCodeCoverage]
public class KafkaTestsCleanUpFixture : IDisposable
{
    private const string BrokerUrl = "localhost:9097";
    private bool disposed;

    public void Dispose()
    {
        this.Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (this.disposed)
        {
            return;
        }

        if (disposing)
        {
            new KafkaClearHelper(BrokerUrl).Clear().Wait();
            StreamingApiClient.Shutdown();
        }

        this.disposed = true;
    }
}
