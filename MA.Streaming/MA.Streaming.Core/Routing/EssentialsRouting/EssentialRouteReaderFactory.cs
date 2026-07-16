// <copyright file="EssentialRouteReaderFactory.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing.EssentialsRouting;

public class EssentialRouteReaderFactory : IEssentialRouteReaderFactory
{
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IRouteManager routeManager;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouteSubscriber> routeReadingWritingComponentRepository;
    private readonly ILogger logger;
    private readonly IKafkaSecurityConfigProvider kafkaSecurityConfigProvider;
    private readonly string filePath;
    private readonly string? whitelistFilePath;

    public EssentialRouteReaderFactory(
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IKafkaRouteInfoCreator kafkaRouteInfoCreator,
        IRouteManager routeManager,
        IRouteReadingWritingComponentRepository<KafkaRouteSubscriber> routeReadingWritingComponentRepository,
        ILogger logger,
        IKafkaSecurityConfigProvider kafkaSecurityConfigProvider)
    {
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.routeManager = routeManager;
        this.routeReadingWritingComponentRepository = routeReadingWritingComponentRepository;
        this.logger = logger;
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.kafkaSecurityConfigProvider = kafkaSecurityConfigProvider;
        var config = streamingApiConfigurationProvider.Provide();
        this.filePath = config.KafkaBrokerConsumingConfigFilePath;
        this.whitelistFilePath = config.KafkaBrokerConfigWhitelistFilePath;
    }

    public IRouteSubscriber Create(ConnectionDetailsDto connectionDetailsDto)
    {
        return this.CreateRouteReader(connectionDetailsDto);
    }

    private KafkaRouteSubscriber CreateRouteReader(ConnectionDetailsDto connectionDetailsDto)
    {
        IConsumingConfigurationProvider provider = new EssentialRouteConsumingConfigurationProvider(
            connectionDetailsDto,
            this.streamingApiConfigurationProvider,
            this.kafkaRouteInfoCreator);

        provider = ConsumingConfigurationProviderCreator.ApplyFileOverride(provider, this.filePath, this.kafkaSecurityConfigProvider, this.whitelistFilePath);

        return new KafkaRouteSubscriber(
            new KafkaListenerFactory(provider, this.logger),
            provider,
            this.routeManager,
            this.logger,
            this.routeReadingWritingComponentRepository,
            $"Essential_Reader_Subscriber_{connectionDetailsDto.Id}");
    }
}
