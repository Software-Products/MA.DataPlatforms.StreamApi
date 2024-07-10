// <copyright file="PartitionBasedRoutingConfigurationProvider.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatform.Secu4.RouterComponent.Abstractions;
using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Routing.EssentialsRouting;

namespace MA.Streaming.Core.Routing.PartitionsRouting;

public class PartitionBasedRoutingConfigurationProvider : IRoutingConfigurationProvider
{
    private readonly string dataSource;
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly IRouteBindingInfoRepository routeBindingInfoRepository;
    private readonly IEssentialTopicNameCreator essentialTopicNameCreator;

    public PartitionBasedRoutingConfigurationProvider(
        string dataSource,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IRouteBindingInfoRepository routeBindingInfoRepository,
        IEssentialTopicNameCreator essentialTopicNameCreator)
    {
        this.dataSource = dataSource;
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.routeBindingInfoRepository = routeBindingInfoRepository;
        this.essentialTopicNameCreator = essentialTopicNameCreator;
    }

    public RoutingConfiguration Provide()
    {
        var config = this.streamingApiConfigurationProvider.Provide();
        var sessionRouteBindingInfo = this.routeBindingInfoRepository.GetOrAdd(Constants.SessionInfoDataSourceName);
        var systemStatus = this.routeBindingInfoRepository.GetOrAdd(Constants.SystemStatusDataSourceName);
        var mainRouteBindingInfo = this.routeBindingInfoRepository.GetOrAdd(this.dataSource);
        var essentialRouteBindingInfo = this.routeBindingInfoRepository.GetOrAdd(this.dataSource, Constants.EssentialStreamName, true);
        var routes = new List<KafkaRoute>
        {
            new(sessionRouteBindingInfo.RouteName, Constants.SessionInfoTopicName),
            new(systemStatus.RouteName, Constants.SystemStatusTopicName),
            new(mainRouteBindingInfo.RouteName, this.dataSource, 0),
            new(essentialRouteBindingInfo.RouteName, this.essentialTopicNameCreator.Create(this.dataSource))
        };
        var hasPartitionsMapping = config is { StreamCreationStrategy: StreamCreationStrategy.PartitionBased, PartitionMappings: not null } &&
                                   config.PartitionMappings.Any();
        if (hasPartitionsMapping)
        {
            routes.AddRange(
                from partitionMapping in config.PartitionMappings ?? []
                let streamRouteBindingInfo = this.routeBindingInfoRepository.GetOrAdd(this.dataSource, partitionMapping.Stream)
                select new KafkaRoute(streamRouteBindingInfo.RouteName, this.dataSource, partitionMapping.Partition));
        }

        var metaData = new List<KafkaTopicMetaData>();
        const int MainPartitionNumber = 1;
        var numberOfPartitions = hasPartitionsMapping
            ? config.PartitionMappings?.Max(i => i.Partition) + MainPartitionNumber
            : MainPartitionNumber;
        metaData.Add(new KafkaTopicMetaData(this.dataSource, numberOfPartitions));

        return new RoutingConfiguration(
            new KafkaRoutingConfig(
                new KafkaPublishingConfig(server: config.BrokerUrl, compressionLevel: -1, compressionType: 4, 0, 10_000_000, int.MaxValue),
                routes,
                metaData,
                "dead-Letter"));
    }
}
