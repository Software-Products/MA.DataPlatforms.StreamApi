// <copyright file="DataFormatConsumingConfigurationProvider.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Abstraction;

namespace MA.Streaming.Core.DataFormatManagement;

public class DataFormatConsumingConfigurationProvider : IConsumingConfigurationProvider
{
    private readonly IReadOnlyList<KafkaRoute> routes;
    protected readonly IStreamingApiConfiguration StreamApiConfig;

    public DataFormatConsumingConfigurationProvider(
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IReadOnlyList<KafkaRoute> routes)
    {
        this.routes = routes;
        this.StreamApiConfig = streamingApiConfigurationProvider.Provide();
    }

    public ConsumingConfiguration Provide()
    {
        var subscriptionConfiguration = this.routes.Select(
            i => new KafkaConsumingConfig(
                new KafkaListeningConfig(
                    this.StreamApiConfig.BrokerUrl,
                    Guid.NewGuid().ToString(),
                    AutoOffsetResetMode.Earliest,
                    0),
                i,
                new KafkaTopicMetaData(i.Topic, 1))
        ).ToList();

        return new ConsumingConfiguration(subscriptionConfiguration);
    }
}
