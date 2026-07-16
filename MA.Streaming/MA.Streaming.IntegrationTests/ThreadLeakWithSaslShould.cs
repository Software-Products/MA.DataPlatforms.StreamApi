// <copyright file="ThreadLeakWithSaslShould.cs" company="Motion Applied Ltd.">
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

using MA.Streaming.Abstraction;
using MA.Streaming.IntegrationTests.Base;
using MA.Streaming.IntegrationTests.Helper;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class ThreadLeakWithSaslShould : ThreadLeakBase
{
    public ThreadLeakWithSaslShould(ITestOutputHelper output) : base(output) { }

    protected override string BrokerUrl => "localhost:9099";
    protected override string DataSource => "SASL_ThreadLeakDataSource";
    protected override string Domain => "SASL_ThreadLeakDomain";

    protected override IKafkaSecurityConfigurationProvider CreateSecurityProvider() =>
        new SaslPlainKafkaSecurityConfigProvider();
}
