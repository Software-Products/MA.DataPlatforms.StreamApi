// <copyright file="EssentialRouterConfigurationProvider.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing.EssentialsRouting;

public class EssentialRouterConfigurationProvider : IEssentialRouterConfigurationProvider
{
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly Dictionary<string, RoutingConfigurationProvider> configurationProviders = new();
    private readonly object readWriteLock = new();

    public EssentialRouterConfigurationProvider(
        IKafkaRouteInfoCreator kafkaRouteInfoCreator,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider)
    {
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
    }

    public RoutingConfigurationProvider Provide(string dataSource)
    {
        lock (this.readWriteLock)
        {
            if (this.configurationProviders.TryGetValue(dataSource, out var existingProvider))
            {
                return existingProvider;
            }

            var configurationProvider = this.CreateConfigurationProvider(dataSource);
            this.configurationProviders[dataSource] = configurationProvider;
            return configurationProvider;
        }
    }

    private RoutingConfigurationProvider CreateConfigurationProvider(string dataSource)
    {
        var config = this.streamingApiConfigurationProvider.Provide();
        var deadLetterRouteInfo = this.kafkaRouteInfoCreator.CreateDeadLetterRouteInfo();
        var essentialRouteInfo = this.kafkaRouteInfoCreator.CreateEssentialRouteInfo(dataSource);
        var routes = new List<KafkaRoute>
        {
            new(essentialRouteInfo.RouteNameInfo.Name, essentialRouteInfo.KafkaRouteTopicInfo.Topic),
        };
        var metaData = new List<KafkaTopicMetaData>
        {
            new(essentialRouteInfo.KafkaRouteTopicInfo.Topic),
        };
        var configuration = new RoutingConfiguration(
            new KafkaRoutingConfig(
                KafkaPublishingConfigCreator.Create(config.BrokerUrl),
                routes,
                metaData,
                deadLetterRouteInfo.KafkaRouteTopicInfo.Topic));
        return new RoutingConfigurationProvider(configuration);
    }
}
