// <copyright file="ConsumingConfigurationProvider.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Core.Routing;

public class ConsumingConfigurationProvider : IConsumingConfigurationProvider
{
    protected readonly ConnectionDetailsDto ConnectionDetailsDto;
    protected readonly IStreamingApiConfiguration StreamApiConfig;
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;

    public ConsumingConfigurationProvider(
        ConnectionDetailsDto connectionDetailsDto,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IKafkaRouteInfoCreator kafkaRouteInfoCreator)
    {
        this.ConnectionDetailsDto = connectionDetailsDto;
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.StreamApiConfig = streamingApiConfigurationProvider.Provide();
    }

    public ConsumingConfiguration Provide()
    {
        var partitionMappings = this.StreamApiConfig.PartitionMappings ?? [];
        var mainRoutePartitions = partitionMappings.Any() ? partitionMappings.Max(i => i.Partition) + 1 : 1;
        var kafkaSubscriptionConfigs = new List<KafkaConsumingConfig>();
        var includeMainStream = !this.ConnectionDetailsDto.ExcludeMainStream;
        if (includeMainStream)
        {
            this.AddMainStreamSubscription(kafkaSubscriptionConfigs, mainRoutePartitions);
        }

        this.AddStreamsConfigurations(mainRoutePartitions, kafkaSubscriptionConfigs);
        return new ConsumingConfiguration(
            kafkaSubscriptionConfigs);
    }

    private void AddMainStreamSubscription(List<KafkaConsumingConfig> kafkaSubscriptionConfigs, int mainRoutePartitions)
    {
        var mainRouteInfo = this.kafkaRouteInfoCreator.CreateMainRouteInfo(this.ConnectionDetailsDto.DataSource);
        var mainRoute = GetMainRoute(mainRouteInfo);
        kafkaSubscriptionConfigs.Add(
            new KafkaConsumingConfig(
                new KafkaListeningConfig(
                    this.StreamApiConfig.BrokerUrl,
                    $"Main_Data_Consuming_{Guid.NewGuid()}",
                    AutoOffsetResetMode.FromOffset,
                    this.ConnectionDetailsDto.MainOffset),
                mainRoute,
                new KafkaTopicMetaData(mainRoute.Topic, mainRoutePartitions)));
    }

    private void AddStreamsConfigurations(int mainTopicPartitionNumbers, List<KafkaConsumingConfig> kafkaSubscriptionConfigs)
    {
        var index = 0;
        var mainRouteInfo = this.kafkaRouteInfoCreator.CreateMainRouteInfo(this.ConnectionDetailsDto.DataSource);
        var mainTopicName = mainRouteInfo.KafkaRouteTopicInfo.Topic;
        foreach (var stream in this.ConnectionDetailsDto.Streams)
        {
            var streamOffset = index < this.ConnectionDetailsDto.StreamOffsets.Count ? this.ConnectionDetailsDto.StreamOffsets[index] : 0;
            var kafkaRouteInfo = this.kafkaRouteInfoCreator.CreateStreamRouteInfo(this.ConnectionDetailsDto.DataSource, stream);
            if (kafkaRouteInfo.RouteNameInfo.RouteType != RouteType.Main)
            {
                var kafkaRoute = new KafkaRoute(
                    kafkaRouteInfo.RouteNameInfo.Name,
                    kafkaRouteInfo.KafkaRouteTopicInfo.Topic,
                    kafkaRouteInfo.KafkaRouteTopicInfo.Partition);
                kafkaSubscriptionConfigs.Add(
                    new KafkaConsumingConfig(
                        new KafkaListeningConfig(
                            this.StreamApiConfig.BrokerUrl,
                            $"Stream_Data_Consuming_{Guid.NewGuid()}",
                            AutoOffsetResetMode.FromOffset,
                            streamOffset),
                        kafkaRoute,
                        GetKafkaTopicMetaData(mainTopicName, mainTopicPartitionNumbers, kafkaRoute)));
            }

            index++;
        }
    }

    private static KafkaTopicMetaData GetKafkaTopicMetaData(string mainTopicName, int mainTopicPartitionNumbers, KafkaRoute streamRoute)
    {
        return mainTopicName == streamRoute.Topic ? new KafkaTopicMetaData(mainTopicName, mainTopicPartitionNumbers) : new KafkaTopicMetaData(streamRoute.Topic, 1);
    }

    private static KafkaRoute GetMainRoute(KafkaRouteInfo mainRouteInfo)
    {
        return new KafkaRoute(mainRouteInfo.RouteNameInfo.Name, mainRouteInfo.KafkaRouteTopicInfo.Topic, mainRouteInfo.KafkaRouteTopicInfo.Partition);
    }
}
