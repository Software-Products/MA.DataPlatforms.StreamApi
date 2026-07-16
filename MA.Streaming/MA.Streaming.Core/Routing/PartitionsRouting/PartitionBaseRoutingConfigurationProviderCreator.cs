// <copyright file="PartitionBaseRouterConfigurationHolderCreator.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Configuration.Core;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.Streaming.Abstraction;

namespace MA.Streaming.Core.Routing.PartitionsRouting;

public class PartitionBaseRoutingConfigurationProviderCreator
{
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IStreamingApiConfiguration streamingApiConfiguration;
    private readonly IKafkaSecurityConfigProvider kafkaSecurityConfigProvider;
    private readonly ILogger? logger;
    private readonly Dictionary<string, IRoutingConfigurationProvider> routingConfigurationHolders = new();
    private readonly object readWriteLock = new();

    public PartitionBaseRoutingConfigurationProviderCreator(
        IKafkaRouteInfoCreator kafkaRouteInfoCreator,
        IStreamingApiConfiguration streamingApiConfiguration,
        IKafkaSecurityConfigProvider? kafkaSecurityConfigProvider = null,
        ILogger? logger = null)
    {
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.streamingApiConfiguration = streamingApiConfiguration;
        this.kafkaSecurityConfigProvider = kafkaSecurityConfigProvider ?? new EmptyKafkaSecurityConfigProvider();
        this.logger = logger;
    }

    public IRoutingConfigurationProvider Create(string dataSource)
    {
        lock (this.readWriteLock)
        {
            if (this.routingConfigurationHolders.TryGetValue(dataSource, out var existingHolder))
            {
                return existingHolder;
            }

            var deadLetterRouteInfo = this.kafkaRouteInfoCreator.CreateDeadLetterRouteInfo();
            var mainRouteInfo = this.kafkaRouteInfoCreator.CreateMainRouteInfo(dataSource);

            var routes = new List<KafkaRoute>
            {
                new(mainRouteInfo.RouteNameInfo.Name, mainRouteInfo.KafkaRouteTopicInfo.Topic, 0),
            };

            var hasPartitionsMapping = this.streamingApiConfiguration is { StreamCreationStrategy: StreamCreationStrategy.PartitionBased, PartitionMappings: not null } && this.streamingApiConfiguration.PartitionMappings.Any();
            if (hasPartitionsMapping)
            {
                routes.AddRange(
                    from partitionMapping in this.streamingApiConfiguration.PartitionMappings ?? []
                    let streamRouteInfo = this.kafkaRouteInfoCreator.CreateStreamRouteInfo(dataSource, partitionMapping.Stream)
                    select new KafkaRoute(streamRouteInfo.RouteNameInfo.Name, streamRouteInfo.KafkaRouteTopicInfo.Topic, streamRouteInfo.KafkaRouteTopicInfo.Partition));
            }

            var metaData = new List<KafkaTopicMetaData>();
            const int MainPartitionNumber = 1;
            var numberOfPartitions = hasPartitionsMapping
                ? this.streamingApiConfiguration.PartitionMappings?.Max(i => i.Partition) + MainPartitionNumber
                : MainPartitionNumber;
            metaData.Add(new KafkaTopicMetaData(mainRouteInfo.KafkaRouteTopicInfo.Topic, numberOfPartitions));

            var configuration = new RoutingConfiguration(
                new KafkaRoutingConfig(
                    KafkaPublishingConfigCreator.Create(this.streamingApiConfiguration.BrokerUrl, this.streamingApiConfiguration.EnableRouterPublishMetrics ? KafkaPublishingConfigCreator.EnabledStatisticsIntervalMs : 0),
                    routes,
                    metaData,
                    deadLetterRouteInfo.KafkaRouteTopicInfo.Topic,
                    this.kafkaSecurityConfigProvider.Provide()));

            IRoutingConfigurationProvider provider = new RoutingConfigurationProvider(configuration);
            provider = RoutingConfigurationProviderCreator.ApplyFileOverride(
                provider,
                this.streamingApiConfiguration.KafkaBrokerPublishingConfigFilePath,
                this.kafkaSecurityConfigProvider,
                this.logger,
                this.streamingApiConfiguration.KafkaBrokerConfigWhitelistFilePath);

            this.routingConfigurationHolders.Add(dataSource, provider);
            return provider;
        }
    }
}
