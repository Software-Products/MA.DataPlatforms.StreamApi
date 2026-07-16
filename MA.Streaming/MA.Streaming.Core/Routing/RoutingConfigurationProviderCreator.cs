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

using System.Text.Json;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Configuration.Core;
using MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;
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
    private readonly IKafkaSecurityConfigProvider kafkaSecurityConfigProvider;
    private readonly ILogger? logger;
    private readonly Dictionary<string, IRoutingConfigurationProvider> mainRoutingConfigurationHolders = new();
    private readonly object mainReadWriteLock = new();
    private readonly TopicBasedRoutingConfigurationProviderCreator topicBasedRoutingConfigurationProviderCreator;
    private readonly PartitionBaseRoutingConfigurationProviderCreator partitionBasedRoutingConfigurationProviderCreator;

    public RoutingConfigurationProviderCreator(IStreamingApiConfigurationProvider apiConfigurationProvider, IKafkaRouteInfoCreator kafkaRouteInfoCreator, IKafkaSecurityConfigProvider kafkaSecurityConfigProvider, ILogger? logger = null)
    {
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.apiConfiguration = apiConfigurationProvider.Provide();
        this.kafkaSecurityConfigProvider = kafkaSecurityConfigProvider;
        this.logger = logger;
        this.topicBasedRoutingConfigurationProviderCreator = new TopicBasedRoutingConfigurationProviderCreator(this.kafkaRouteInfoCreator, this.apiConfiguration, kafkaSecurityConfigProvider, logger);
        this.partitionBasedRoutingConfigurationProviderCreator = new PartitionBaseRoutingConfigurationProviderCreator(this.kafkaRouteInfoCreator, this.apiConfiguration, kafkaSecurityConfigProvider, logger);
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
                    KafkaPublishingConfigCreator.Create(this.apiConfiguration.BrokerUrl, this.apiConfiguration.EnableRouterPublishMetrics ? KafkaPublishingConfigCreator.EnabledStatisticsIntervalMs : 0),
                    routes,
                    metaData,
                    deadLetterRouteInfo.KafkaRouteTopicInfo.Topic,
                    this.kafkaSecurityConfigProvider.Provide()));

            IRoutingConfigurationProvider provider = new RoutingConfigurationProvider(configuration);
            provider = ApplyFileOverride(provider, this.apiConfiguration.KafkaBrokerPublishingConfigFilePath, this.kafkaSecurityConfigProvider, this.logger, this.apiConfiguration.KafkaBrokerConfigWhitelistFilePath);

            this.mainRoutingConfigurationHolders[dataSource] = provider;
            return provider;
        }
    }

    public IRoutingConfigurationProvider CreateDataStreamProvider(string dataSource, string stream)
    {
        return this.apiConfiguration.StreamCreationStrategy == StreamCreationStrategy.TopicBased
            ? this.topicBasedRoutingConfigurationProviderCreator.Create(dataSource, stream)
            : this.partitionBasedRoutingConfigurationProviderCreator.Create(dataSource);
    }

    internal static IRoutingConfigurationProvider ApplyFileOverride(
        IRoutingConfigurationProvider inner,
        string filePath,
        IKafkaSecurityConfigProvider securityConfigProvider,
        ILogger? logger = null,
        string? whitelistFilePath = null)
    {
        var fullPath = Path.GetFullPath(filePath);
        if (!File.Exists(filePath))
        {
            logger?.Info(
                $"[Kafka publisher] broker config file NOT found at '{fullPath}' — using Stream API defaults " +
                "(compression.type=zstd, linger.ms=20, batch.size=4194304, acks=1; see KafkaPublishingConfigCreator for full defaults).");
            return inner;
        }

        var whitelist = LoadWhitelistRules(whitelistFilePath, "Publish");
        var provider = new JsonKafkaPublisherBrokerOverrideProvider(
            inner,
            filePath,
            excludeFields: PublisherBrokerFields.BootstrapServers,
            securityConfigProvider: securityConfigProvider,
            whitelist: whitelist);

        var props = provider.Provide().KafkaRoutingConfig.KafkaPublishingConfig.Properties;
        logger?.Info(
            $"[Kafka publisher] applied broker config file '{fullPath}': " +
            $"compression.type={props.GetValueOrDefault("compression.type", "(default)")}, " +
            $"linger.ms={props.GetValueOrDefault("linger.ms", "(default)")}, " +
            $"batch.size={props.GetValueOrDefault("batch.size", "(default)")}, " +
            $"acks={props.GetValueOrDefault("acks", "(default)")}, " +
            $"queue.buffering.max.messages={props.GetValueOrDefault("queue.buffering.max.messages", "(default)")}, " +
            $"queue.buffering.max.kbytes={props.GetValueOrDefault("queue.buffering.max.kbytes", "(default)")}.");
        return provider;
    }

    internal static IReadOnlyList<KafkaConfigOverrideRule>? LoadWhitelistRules(string? path, string section)
    {
        if (string.IsNullOrEmpty(path) || !File.Exists(path))
            return null;
        try
        {
            using var stream = File.OpenRead(path);
            using var doc = JsonDocument.Parse(stream);
            if (!doc.RootElement.TryGetProperty(section, out var el))
                return null;
            return JsonSerializer.Deserialize<List<KafkaConfigOverrideRule>>(
                el.GetRawText(),
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        }
        catch
        {
            return null;
        }
    }
}
