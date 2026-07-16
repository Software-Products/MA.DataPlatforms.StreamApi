// <copyright file="EssentialRouteConsumingConfigurationProvider.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Core.Routing.EssentialsRouting;

public class EssentialRouteConsumingConfigurationProvider : IConsumingConfigurationProvider
{
    private readonly ConnectionDetailsDto connectionDetailsDto;
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IStreamingApiConfiguration streamApiConfig;

    public EssentialRouteConsumingConfigurationProvider(
        ConnectionDetailsDto connectionDetailsDto,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IKafkaRouteInfoCreator kafkaRouteInfoCreator)
    {
        this.connectionDetailsDto = connectionDetailsDto;
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.streamApiConfig = streamingApiConfigurationProvider.Provide();
    }

    public ConsumingConfiguration Provide()
    {
        var kafkaRouteInfo = this.kafkaRouteInfoCreator.CreateEssentialRouteInfo(this.connectionDetailsDto.DataSource);
        var essentialRoute = GetEssentialRoute(kafkaRouteInfo);
        var essentialReaderGroupName = $"Essential_Reader_{Guid.NewGuid()}";
        var kafkaSubscriptionConfigs = new List<KafkaConsumingConfig>
        {
            new(
                KafkaListeningConfigCreator.Create(
                    this.streamApiConfig.BrokerUrl,
                    essentialReaderGroupName,
                    AutoOffsetResetMode.FromOffset,
                    this.connectionDetailsDto.EssentialsOffset,
                    statisticsIntervalMs: this.streamApiConfig.EnableRouterConsumeMetrics ? KafkaPublishingConfigCreator.EnabledStatisticsIntervalMs : 0),
                essentialRoute,
                new KafkaTopicMetaData(essentialRoute.Topic, 1))
        };
        return new ConsumingConfiguration(kafkaSubscriptionConfigs);
    }

    private static KafkaRoute GetEssentialRoute(KafkaRouteInfo kafkaRouteInfo)
    {
        return new KafkaRoute(
            kafkaRouteInfo.RouteNameInfo.Name,
            kafkaRouteInfo.KafkaRouteTopicInfo.Topic,
            kafkaRouteInfo.KafkaRouteTopicInfo.Partition);
    }
}
