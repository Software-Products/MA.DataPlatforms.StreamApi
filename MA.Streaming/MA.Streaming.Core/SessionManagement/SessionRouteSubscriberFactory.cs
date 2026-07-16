// <copyright file="SessionRouteSubscriberFactory.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouteSubscriberComponent;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Configuration.Core;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.SessionManagement;

public class SessionRouteSubscriberFactory : ISessionRouteSubscriberFactory
{
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly IRouteManager routeManager;
    private readonly ILogger logger;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouteSubscriber> readingWritingComponentRepository;
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IKafkaSecurityConfigProvider kafkaSecurityConfigProvider;
    private readonly string filePath;
    private readonly string? whitelistFilePath;

    public SessionRouteSubscriberFactory(
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IRouteManager routeManager,
        IKafkaRouteInfoCreator kafkaRouteInfoCreator,
        ILogger logger,
        IRouteReadingWritingComponentRepository<KafkaRouteSubscriber> readingWritingComponentRepository,
        IKafkaSecurityConfigProvider kafkaSecurityConfigProvider)
    {
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.routeManager = routeManager;
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.logger = logger;
        this.readingWritingComponentRepository = readingWritingComponentRepository;
        this.kafkaSecurityConfigProvider = kafkaSecurityConfigProvider;
        var config = streamingApiConfigurationProvider.Provide();
        this.filePath = config.KafkaBrokerConsumingConfigFilePath;
        this.whitelistFilePath = config.KafkaBrokerConfigWhitelistFilePath;
    }

    public IRouteSubscriber Create()
    {
        IConsumingConfigurationProvider provider = new SessionInfoConsumingConfigurationProvider(
            this.kafkaRouteInfoCreator,
            this.streamingApiConfigurationProvider);

        provider = ConsumingConfigurationProviderCreator.ApplyFileOverride(provider, this.filePath, this.kafkaSecurityConfigProvider, this.whitelistFilePath);

        return new KafkaRouteSubscriber(
            new KafkaListenerFactory(provider, this.logger),
            provider,
            this.routeManager,
            this.logger,
            this.readingWritingComponentRepository,
            "SessionInfo_Route_Reader");
    }
}
