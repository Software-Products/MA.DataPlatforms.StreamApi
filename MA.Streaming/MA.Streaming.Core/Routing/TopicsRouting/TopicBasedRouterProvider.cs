// <copyright file="TopicBasedRouterProvider.cs" company="McLaren Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouterComponent;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing.EssentialsRouting;
using MA.Streaming.Core.Routing.PartitionsRouting;

namespace MA.Streaming.Core.Routing.TopicsRouting;

public class TopicBasedRouterProvider : IRouterProvider
{
    private readonly ILogger routerLogger;
    private readonly IStreamingApiConfigurationProvider configurationProvider;
    private readonly IRouteBindingInfoRepository routeBindingInfoRepository;
    private readonly IEssentialTopicNameCreator essentialTopicNameCreator;
    private readonly ITopicBaseTopicNameCreator topicBaseTopicNameCreator;
    private readonly IRouteManager routeManager;

    private readonly object readerWriterLock = new();
    private readonly Dictionary<string, IRouter> routers = new();

    public TopicBasedRouterProvider(
        ILogger routerLogger,
        IStreamingApiConfigurationProvider configurationProvider,
        IRouteBindingInfoRepository routeBindingInfoRepository,
        IEssentialTopicNameCreator essentialTopicNameCreator,
        ITopicBaseTopicNameCreator topicBaseTopicNameCreator,
        IRouteManager routeManager)
    {
        this.routerLogger = routerLogger;
        this.configurationProvider = configurationProvider;
        this.routeBindingInfoRepository = routeBindingInfoRepository;
        this.essentialTopicNameCreator = essentialTopicNameCreator;
        this.topicBaseTopicNameCreator = topicBaseTopicNameCreator;
        this.routeManager = routeManager;
    }

    public IRouter Provide(string dataSource, string stream = "")
    {
        lock (this.readerWriterLock)
        {
            if (!this.routers.TryGetValue(dataSource, out var mainRouter))
            {
                mainRouter = this.CreateMainTopicRouter(dataSource);
                this.routers.Add(dataSource, mainRouter);
            }

            if (string.IsNullOrEmpty(stream))
            {
                return mainRouter;
            }

            if (this.routers.TryGetValue(this.topicBaseTopicNameCreator.Create(dataSource, stream), out var streamRouter))
            {
                return streamRouter;
            }

            streamRouter = this.CreateStreamTopicRouter(dataSource, stream);
            this.routers.Add(this.topicBaseTopicNameCreator.Create(dataSource, stream), streamRouter);

            return streamRouter;
        }
    }

    private IRouter CreateStreamTopicRouter(string dataSource, string stream)
    {
        var router = new Router(
            this.routerLogger,
            new KafkaProducerBuilder(
                this.routerLogger,
                new TopicBasedRoutingConfigurationProvider(
                    dataSource,
                    stream,
                    this.configurationProvider,
                    this.routeBindingInfoRepository,
                    this.topicBaseTopicNameCreator),
                this.routeManager));
        router.Initiate();
        return router;
    }

    private IRouter CreateMainTopicRouter(string dataSource)
    {
        var router = new Router(
            this.routerLogger,
            new KafkaProducerBuilder(
                this.routerLogger,
                new PartitionBasedRoutingConfigurationProvider(
                    dataSource,
                    this.configurationProvider,
                    this.routeBindingInfoRepository,
                    this.essentialTopicNameCreator),
                this.routeManager));
        router.Initiate();
        return router;
    }
}
