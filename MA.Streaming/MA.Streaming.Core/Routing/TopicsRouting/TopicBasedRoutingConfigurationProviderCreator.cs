// <copyright file="TopicBasedRouterConfigurationHolderCreator.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Core.Routing.TopicsRouting;

public class TopicBasedRoutingConfigurationProviderCreator
{
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IStreamingApiConfiguration streamingApiConfiguration;
    private readonly IKafkaSecurityConfigProvider kafkaSecurityConfigProvider;
    private readonly ILogger? logger;
    private readonly Dictionary<string, IRoutingConfigurationProvider> configurationProviders = new();
    private readonly object readWriteLock = new();

    public TopicBasedRoutingConfigurationProviderCreator(
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

    public IRoutingConfigurationProvider Create(string dataSource, string stream)
    {
        lock (this.readWriteLock)
        {
            var routeStreamInfo = this.kafkaRouteInfoCreator.CreateStreamRouteInfo(dataSource, stream);
            if (this.configurationProviders.TryGetValue(routeStreamInfo.KafkaRouteTopicInfo.Topic, out var existingProvider))
            {
                return existingProvider;
            }

            var provider = this.CreateConfigurationProvider(dataSource, stream);
            this.configurationProviders[routeStreamInfo.KafkaRouteTopicInfo.Topic] = provider;
            return provider;
        }
    }

    private IRoutingConfigurationProvider CreateConfigurationProvider(string dataSource, string stream)
    {
        var deadLetterRouteInfo = this.kafkaRouteInfoCreator.CreateDeadLetterRouteInfo();
        var streamDataRouterInfo = this.kafkaRouteInfoCreator.CreateStreamRouteInfo(dataSource, stream);
        var routes = new List<KafkaRoute>
        {
            new(streamDataRouterInfo.RouteNameInfo.Name, streamDataRouterInfo.KafkaRouteTopicInfo.Topic),
        };
        var metaData = new List<KafkaTopicMetaData>
        {
            new(streamDataRouterInfo.KafkaRouteTopicInfo.Topic),
        };
        var configuration = new RoutingConfiguration(
            new KafkaRoutingConfig(
                KafkaPublishingConfigCreator.Create(this.streamingApiConfiguration.BrokerUrl, this.streamingApiConfiguration.EnableRouterPublishMetrics ? KafkaPublishingConfigCreator.EnabledStatisticsIntervalMs : 0),
                routes,
                metaData,
                deadLetterRouteInfo.KafkaRouteTopicInfo.Topic,
                this.kafkaSecurityConfigProvider.Provide()));

        IRoutingConfigurationProvider provider = new RoutingConfigurationProvider(configuration);
        return RoutingConfigurationProviderCreator.ApplyFileOverride(
            provider,
            this.streamingApiConfiguration.KafkaBrokerPublishingConfigFilePath,
            this.kafkaSecurityConfigProvider,
            this.logger,
            this.streamingApiConfiguration.KafkaBrokerConfigWhitelistFilePath);
    }
}
