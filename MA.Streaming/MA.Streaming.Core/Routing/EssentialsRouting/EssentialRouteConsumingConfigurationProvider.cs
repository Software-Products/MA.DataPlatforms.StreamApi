// <copyright file="EssentialRouteConsumingConfigurationProvider.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.Contracts;

namespace MA.Streaming.Core.Routing.EssentialsRouting;

public class EssentialRouteConsumingConfigurationProvider : IConsumingConfigurationProvider
{
    private readonly ConnectionDetailsDto connectionDetailsDto;
    private readonly IRouteBindingInfoRepository routeBindingInfoRepository;
    private readonly IEssentialTopicNameCreator essentialTopicNameCreator;
    private readonly IStreamingApiConfiguration streamApiConfig;

    public EssentialRouteConsumingConfigurationProvider(
        ConnectionDetailsDto connectionDetailsDto,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IRouteBindingInfoRepository routeBindingInfoRepository,
        IEssentialTopicNameCreator essentialTopicNameCreator)
    {
        this.connectionDetailsDto = connectionDetailsDto;
        this.routeBindingInfoRepository = routeBindingInfoRepository;
        this.essentialTopicNameCreator = essentialTopicNameCreator;
        this.streamApiConfig = streamingApiConfigurationProvider.Provide();
    }

    public ConsumingConfiguration Provide()
    {
        var essentialRoute = this.GetEssentialRoute();
        var kafkaSubscriptionConfigs = new List<KafkaConsumingConfig>
        {
            new(
                new KafkaListeningConfig(
                    this.streamApiConfig.BrokerUrl,
                    Guid.NewGuid().ToString(),
                    AutoOffsetResetMode.FromOffset,
                    this.connectionDetailsDto.MainOffset),
                essentialRoute,
                new KafkaTopicMetaData(essentialRoute.Topic, 1))
        };
        return new ConsumingConfiguration(
            kafkaSubscriptionConfigs);
    }

    private KafkaRoute GetEssentialRoute()
    {
        var essentialBindingInfo = this.routeBindingInfoRepository.GetOrAdd(this.connectionDetailsDto.DataSource, Constants.EssentialStreamName, true);
        var essentialRoute = new KafkaRoute(
            essentialBindingInfo.RouteName,
            this.essentialTopicNameCreator.Create(this.connectionDetailsDto.DataSource),
            0);
        return essentialRoute;
    }
}
