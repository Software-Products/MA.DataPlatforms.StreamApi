// <copyright file="RoutingConfigurationProviderCreator.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing.PartitionsRouting;
using MA.Streaming.Core.Routing.TopicsRouting;

namespace MA.Streaming.Core.Routing;

public class RoutingConfigurationProviderCreator : IRoutingConfigurationProviderCreator
{
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IStreamingApiConfiguration apiConfiguration;
    private readonly Dictionary<string, RoutingConfigurationProvider> mainRoutingConfigurationHolders = new();
    private readonly object mainReadWriteLock = new();
    private readonly TopicBasedRoutingConfigurationProviderCreator topicBasedRoutingConfigurationProviderCreator;
    private readonly PartitionBaseRoutingConfigurationProviderCreator partitionBasedRoutingConfigurationProviderCreator;

    public RoutingConfigurationProviderCreator(IStreamingApiConfigurationProvider apiConfigurationProvider, IKafkaRouteInfoCreator kafkaRouteInfoCreator)
    {
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.apiConfiguration = apiConfigurationProvider.Provide();
        this.topicBasedRoutingConfigurationProviderCreator = new TopicBasedRoutingConfigurationProviderCreator(this.kafkaRouteInfoCreator, this.apiConfiguration);
        this.partitionBasedRoutingConfigurationProviderCreator = new PartitionBaseRoutingConfigurationProviderCreator(this.kafkaRouteInfoCreator, this.apiConfiguration);
    }

    public IRoutingConfigurationProvider CreateMainStreamProvider(string dataSource)
    {
        lock (this.mainReadWriteLock)
        {
            if (this.mainRoutingConfigurationHolders.TryGetValue(dataSource, out var existingHolder))
            {
                return existingHolder;
            }

            var deadLetterRouteInfo = this.kafkaRouteInfoCreator.CreateDeadLetterRouteInfo();
            var mainRouteInfo = this.kafkaRouteInfoCreator.CreateMainRouteInfo(dataSource);
            var routes = new List<KafkaRoute>
            {
                new(mainRouteInfo.RouteNameInfo.Name, mainRouteInfo.KafkaRouteTopicInfo.Topic, 0),
            };
            var hasPartitionsMapping = this.apiConfiguration is { StreamCreationStrategy: StreamCreationStrategy.PartitionBased, PartitionMappings: not null } &&
                                       this.apiConfiguration.PartitionMappings.Any();

            var metaData = new List<KafkaTopicMetaData>();
            const int MainPartitionCount = 1;
            var numberOfPartitions = hasPartitionsMapping
                ? this.apiConfiguration.PartitionMappings?.Max(i => i.Partition) + MainPartitionCount
                : MainPartitionCount;
            metaData.Add(new KafkaTopicMetaData(mainRouteInfo.KafkaRouteTopicInfo.Topic, numberOfPartitions));

            var configuration = new RoutingConfiguration(
                new KafkaRoutingConfig(
                    KafkaPublishingConfigCreator.Create(this.apiConfiguration.BrokerUrl),
                    routes,
                    metaData,
                    deadLetterRouteInfo.KafkaRouteTopicInfo.Topic));
            var mainRoutingConfigurationHolder = new RoutingConfigurationProvider(configuration);
            this.mainRoutingConfigurationHolders[dataSource] = mainRoutingConfigurationHolder;
            return mainRoutingConfigurationHolder;
        }
    }

    public IRoutingConfigurationProvider CreateDataStreamProvider(string dataSource, string stream)
    {
        return this.apiConfiguration.StreamCreationStrategy == StreamCreationStrategy.TopicBased
            ? this.topicBasedRoutingConfigurationProviderCreator.Create(dataSource, stream)
            : this.partitionBasedRoutingConfigurationProviderCreator.Create(dataSource);
    }
}
