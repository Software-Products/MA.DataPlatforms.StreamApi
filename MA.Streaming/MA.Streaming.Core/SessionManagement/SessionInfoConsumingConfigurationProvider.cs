// <copyright file="SessionInfoConsumingConfigurationProvider.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Routing;

namespace MA.Streaming.Core.SessionManagement;

public class SessionInfoConsumingConfigurationProvider : IConsumingConfigurationProvider
{
    protected readonly IStreamingApiConfiguration StreamApiConfig;
    private readonly KafkaRouteInfo kafkaRouteInfo;

    public SessionInfoConsumingConfigurationProvider(
        IKafkaRouteInfoCreator kafkaRouteInfoCreator,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider)
    {
        this.StreamApiConfig = streamingApiConfigurationProvider.Provide();
        this.kafkaRouteInfo = kafkaRouteInfoCreator.CreateSessionInfoRouteInfo();
    }

    public ConsumingConfiguration Provide()
    {
        var sessionRoute = this.SessionRoute();
        var kafkaSubscriptionConfigs = new List<KafkaConsumingConfig>
        {
            new(
                KafkaListeningConfigCreator.Create(
                    this.StreamApiConfig.BrokerUrl,
                    $"Session_Info_Consuming_{Guid.NewGuid()}",
                    AutoOffsetResetMode.Earliest,
                    0,
                    statisticsIntervalMs: this.StreamApiConfig.EnableRouterConsumeMetrics ? KafkaPublishingConfigCreator.EnabledStatisticsIntervalMs : 0),
                sessionRoute,
                new KafkaTopicMetaData(sessionRoute.Topic, 1))
        };
        return new ConsumingConfiguration(kafkaSubscriptionConfigs);
    }

    private KafkaRoute SessionRoute()
    {
        return new KafkaRoute(this.kafkaRouteInfo.RouteNameInfo.Name, this.kafkaRouteInfo.KafkaRouteTopicInfo.Topic);
    }
}
