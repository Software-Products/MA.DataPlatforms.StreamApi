// <copyright file="DataFormatRouteSubscriberFactory.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.DataFormatManagement;

public class DataFormatRouteSubscriberFactory : IDataFormatRouteSubscriberFactory
{
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly ILogger logger;
    private readonly IRouteManager routeManager;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouteSubscriber> readingWritingComponentRepository;
    private readonly IDomainNameProvider domainNameProvider;

    public DataFormatRouteSubscriberFactory(
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        ILogger logger,
        IRouteManager routeManager,
        IRouteReadingWritingComponentRepository<KafkaRouteSubscriber> readingWritingComponentRepository,
        IDomainNameProvider domainNameProvider
    )
    {
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.logger = logger;
        this.routeManager = routeManager;
        this.readingWritingComponentRepository = readingWritingComponentRepository;
        this.domainNameProvider = domainNameProvider;
    }

    public IRouteSubscriber Create(IReadOnlyList<KafkaRoute> routes)
    {
        var dataFormatSubscriptionConfigurationProvider = new DataFormatConsumingConfigurationProvider(this.streamingApiConfigurationProvider, routes);
        var kafkaListenerFactory = new KafkaListenerFactory(
            dataFormatSubscriptionConfigurationProvider,
            this.logger);
        return new KafkaRouteSubscriber(
            kafkaListenerFactory,
            dataFormatSubscriptionConfigurationProvider,
            this.routeManager,
            this.logger,
            this.readingWritingComponentRepository,
            $"DataFormat_Route_Subscriber_{this.domainNameProvider.Provide()}_{DateTime.Now.Ticks}");
    }
}
