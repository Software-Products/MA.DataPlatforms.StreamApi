// <copyright file="DomainRouterProvider.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouterComponent;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatforms.Secu4.Routing.Configuration.Core;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing;

public class DomainRouterProvider : IDomainRouterProvider
{
    private readonly ILogger logger;
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IRouteManager routeManager;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouter> readingWritingComponentRepository;
    private readonly IDomainNameProvider domainNameProvider;
    private readonly IKafkaSecurityConfigProvider kafkaSecurityConfigProvider;

    private KafkaRouter? domainRouter;

    private readonly object readWriteLock = new();

    public DomainRouterProvider(
        ILogger logger,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IKafkaRouteInfoCreator kafkaRouteInfoCreator,
        IRouteManager routeManager,
        IRouteReadingWritingComponentRepository<KafkaRouter> readingWritingComponentRepository,
        IDomainNameProvider domainNameProvider,
        IKafkaSecurityConfigProvider kafkaSecurityConfigProvider)
    {
        this.logger = logger;
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.routeManager = routeManager;
        this.readingWritingComponentRepository = readingWritingComponentRepository;
        this.domainNameProvider = domainNameProvider;
        this.kafkaSecurityConfigProvider = kafkaSecurityConfigProvider;
    }

    public IRouter Provide()
    {
        lock (this.readWriteLock)
        {
            if (this.domainRouter != null)
            {
                return this.domainRouter;
            }

            var configurationProvider = this.CreateRoutingConfigurationProvider();

            this.domainRouter = new KafkaRouter(
                this.logger,
                new KafkaProducerBuilder(
                    this.logger,
                    configurationProvider,
                    this.routeManager),
                this.readingWritingComponentRepository,
                $"Domain_Router_{this.domainNameProvider.Provide()}_{DateTime.Now.Ticks}");
            this.domainRouter.Initiate();
            return this.domainRouter;
        }
    }

    private IRoutingConfigurationProvider CreateRoutingConfigurationProvider()
    {
        var config = this.streamingApiConfigurationProvider.Provide();
        var sessionInfoRouteInfo = this.kafkaRouteInfoCreator.CreateSessionInfoRouteInfo();
        var systemStatusRouteInfo = this.kafkaRouteInfoCreator.CreateSystemStatusRouteInfo();
        var deadLetterRouteInfo = this.kafkaRouteInfoCreator.CreateDeadLetterRouteInfo();

        var routes = new List<KafkaRoute>
        {
            new(sessionInfoRouteInfo.RouteNameInfo.Name, sessionInfoRouteInfo.KafkaRouteTopicInfo.Topic),
            new(systemStatusRouteInfo.RouteNameInfo.Name, systemStatusRouteInfo.KafkaRouteTopicInfo.Topic)
        };

        var metaData = new List<KafkaTopicMetaData>
        {
            new(sessionInfoRouteInfo.KafkaRouteTopicInfo.Topic),
            new(systemStatusRouteInfo.KafkaRouteTopicInfo.Topic)
        };

        var routingConfiguration = new RoutingConfiguration(
            new KafkaRoutingConfig(
                KafkaPublishingConfigCreator.Create(config.BrokerUrl, config.EnableRouterPublishMetrics ? KafkaPublishingConfigCreator.EnabledStatisticsIntervalMs : 0),
                routes,
                metaData,
                deadLetterRouteInfo.KafkaRouteTopicInfo.Topic,
                this.kafkaSecurityConfigProvider.Provide()));

        IRoutingConfigurationProvider configurationProvider = new RoutingConfigurationProvider(routingConfiguration);
        return RoutingConfigurationProviderCreator.ApplyFileOverride(
            configurationProvider,
            config.KafkaBrokerPublishingConfigFilePath,
            this.kafkaSecurityConfigProvider,
            whitelistFilePath: config.KafkaBrokerConfigWhitelistFilePath);
    }
}
