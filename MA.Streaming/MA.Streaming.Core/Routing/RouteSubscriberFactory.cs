// <copyright file="RouteSubscriberFactory.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing;

public class RouteSubscriberFactory : IRouteSubscriberFactory
{
    private readonly ILogger logger;
    private readonly IRouteManager routeManager;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouteSubscriber> routeReadingWritingComponentRepository;
    private readonly IConsumingConfigurationProviderCreator consumingConfigurationProviderCreator;

    public RouteSubscriberFactory(
        IConsumingConfigurationProviderCreator consumingConfigurationProviderCreator,
        ILogger logger,
        IRouteManager routeManager,
        IRouteReadingWritingComponentRepository<KafkaRouteSubscriber> routeReadingWritingComponentRepository)
    {
        this.consumingConfigurationProviderCreator = consumingConfigurationProviderCreator;
        this.logger = logger;
        this.routeManager = routeManager;
        this.routeReadingWritingComponentRepository = routeReadingWritingComponentRepository;
    }

    public IRouteSubscriber Create(ConnectionDetailsDto connectionDetailsDto)
    {
        var consumingConfigurationProvider = this.consumingConfigurationProviderCreator.Create(connectionDetailsDto);

        return new KafkaRouteSubscriber(
            new KafkaListenerFactory(
                consumingConfigurationProvider,
                this.logger),
            consumingConfigurationProvider,
            this.routeManager,
            this.logger,
            this.routeReadingWritingComponentRepository,
            $"Data_Reader_Subscriber_{connectionDetailsDto.Id}");
    }
}
