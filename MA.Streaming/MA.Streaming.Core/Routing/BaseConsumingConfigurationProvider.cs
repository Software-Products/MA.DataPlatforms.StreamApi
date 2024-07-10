// <copyright file="BaseConsumingConfigurationProvider.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.DataPlatform.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;

namespace MA.Streaming.Core.Routing;

public abstract class BaseConsumingConfigurationProvider : IConsumingConfigurationProvider
{
    protected readonly ConnectionDetailsDto ConnectionDetailsDto;
    protected readonly IRouteBindingInfoRepository RouteBindingInfoRepository;
    protected readonly IStreamingApiConfiguration StreamApiConfig;

    protected BaseConsumingConfigurationProvider(
        ConnectionDetailsDto connectionDetailsDto,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IRouteBindingInfoRepository routeBindingInfoRepository)
    {
        this.ConnectionDetailsDto = connectionDetailsDto;
        this.RouteBindingInfoRepository = routeBindingInfoRepository;
        this.StreamApiConfig = streamingApiConfigurationProvider.Provide();
    }

    public ConsumingConfiguration Provide()
    {
        var mainRoute = this.GetMainRoute();
        var partitionMappings = this.StreamApiConfig.PartitionMappings ?? [];
        var mainRoutePartitions = partitionMappings.Any() ? partitionMappings.Max(i => i.Partition) + 1 : 1;
        var kafkaSubscriptionConfigs = new List<KafkaConsumingConfig>
        {
            new(
                new KafkaListeningConfig(
                    this.StreamApiConfig.BrokerUrl,
                    Guid.NewGuid().ToString(),
                    AutoOffsetResetMode.FromOffset,
                    this.ConnectionDetailsDto.MainOffset),
                mainRoute,
                new KafkaTopicMetaData(mainRoute.Topic, mainRoutePartitions))
        };
        this.AddStreamsConfigurations(mainRoute.Topic, mainRoutePartitions, kafkaSubscriptionConfigs);

        return new ConsumingConfiguration(
            kafkaSubscriptionConfigs);
    }

    protected abstract KafkaRoute GetStreamRoute(string stream);

    private void AddStreamsConfigurations(string mainTopicName, int mainTopicPartitionNumbers, List<KafkaConsumingConfig> kafkaSubscriptionConfigs)
    {
        var streams = this.ConnectionDetailsDto.Streams;
        if (this.StreamApiConfig.StreamCreationStrategy == StreamCreationStrategy.PartitionBased)
        {
            if (this.StreamApiConfig.PartitionMappings != null &&
                this.StreamApiConfig.PartitionMappings.Any())
            {
                streams = this.ConnectionDetailsDto.Streams.Where(i => (this.StreamApiConfig?.PartitionMappings ?? []).Select(j => j.Stream).Contains(i)).ToList();
            }
            else
            {
                streams = [];
            }
        }

        kafkaSubscriptionConfigs.AddRange(
            streams
                .Select(
                    (stream, i) =>
                    {
                        var streamRoute = this.GetStreamRoute(stream);
                        return new KafkaConsumingConfig(
                            new KafkaListeningConfig(
                                this.StreamApiConfig.BrokerUrl,
                                Guid.NewGuid().ToString(),
                                AutoOffsetResetMode.FromOffset,
                                this.ConnectionDetailsDto.StreamOffsets[i]),
                            streamRoute,
                            GetKafkaTopicMetaData(mainTopicName, mainTopicPartitionNumbers, streamRoute));
                    }));
    }

    private static KafkaTopicMetaData GetKafkaTopicMetaData(string mainTopicName, int mainTopicPartitionNumbers, KafkaRoute streamRoute)
    {
        return mainTopicName == streamRoute.Topic ? new KafkaTopicMetaData(mainTopicName, mainTopicPartitionNumbers) : new KafkaTopicMetaData(streamRoute.Topic, 1);
    }

    private KafkaRoute GetMainRoute()
    {
        var mainBindingInfo = this.RouteBindingInfoRepository.GetOrAdd(this.ConnectionDetailsDto.DataSource);
        if (!this.ConnectionDetailsDto.Streams.Any())
        {
            var mainRoute = new KafkaRoute(
                mainBindingInfo.RouteName,
                this.ConnectionDetailsDto.DataSource);
            return mainRoute;
        }

        var kafkaRoute = new KafkaRoute(
            mainBindingInfo.RouteName,
            this.ConnectionDetailsDto.DataSource,
            0);
        return kafkaRoute;
    }
}
