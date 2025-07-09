// <copyright file="TopicBasedRouteSubscriberFactory.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing.TopicsRouting;

public class TopicBasedRouteSubscriberFactory : IRouteSubscriberFactory
{
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly ITopicBaseTopicNameCreator topicBaseTopicNameCreator;
    private readonly ILogger logger;
    private readonly IRouteManager routeManager;

    public TopicBasedRouteSubscriberFactory(
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        ITopicBaseTopicNameCreator topicBaseTopicNameCreator,
        ILogger logger,
        IRouteManager routeManager)
    {
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.topicBaseTopicNameCreator = topicBaseTopicNameCreator;
        this.logger = logger;
        this.routeManager = routeManager;
    }

    public IRouteSubscriber Create(ConnectionDetailsDto connectionDetailsDto, IRouteBindingInfoRepository routeBindingInfoRepository)
    {
        var topicBasedConsumingConfigurationProvider = new TopicBasedConsumingConfigurationProvider(
            connectionDetailsDto,
            this.streamingApiConfigurationProvider,
            routeBindingInfoRepository,
            this.topicBaseTopicNameCreator);

        return new KafkaRouteSubscriber(
            new KafkaListenerFactory(
                topicBasedConsumingConfigurationProvider,
                new CancellationTokenSourceProvider(),
                this.logger),
            topicBasedConsumingConfigurationProvider,
            this.routeManager);
    }
}
