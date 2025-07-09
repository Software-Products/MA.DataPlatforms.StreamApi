// <copyright file="PartitionBasedRouterProvider.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Core.Routing.PartitionsRouting;

public class PartitionBasedRouterProvider : IRouterProvider
{
    private readonly ILogger logger;
    private readonly IStreamingApiConfigurationProvider configurationProvider;
    private readonly IRouteBindingInfoRepository routeBindingInfoRepository;
    private readonly IEssentialTopicNameCreator essentialTopicNameCreator;
    private readonly IRouteManager routeManager;
    private IRouter? partitionBasedRouter;

    public PartitionBasedRouterProvider(
        ILogger logger,
        IStreamingApiConfigurationProvider configurationProvider,
        IRouteBindingInfoRepository routeBindingInfoRepository,
        IEssentialTopicNameCreator essentialTopicNameCreator,
        IRouteManager routeManager)
    {
        this.logger = logger;
        this.configurationProvider = configurationProvider;
        this.routeBindingInfoRepository = routeBindingInfoRepository;
        this.essentialTopicNameCreator = essentialTopicNameCreator;
        this.routeManager = routeManager;
    }

    public IRouter Provide(string dataSource, string stream = "")
    {
        if (this.partitionBasedRouter != null)
        {
            return this.partitionBasedRouter;
        }

        this.partitionBasedRouter = this.CreateRouter(dataSource);

        return this.partitionBasedRouter;
    }

    private IRouter CreateRouter(string dataSource)
    {
        var router = new Router(
            this.logger,
            new KafkaProducerBuilder(
                this.logger,
                new PartitionBasedRoutingConfigurationProvider(dataSource, this.configurationProvider, this.routeBindingInfoRepository, this.essentialTopicNameCreator),
                this.routeManager));
        router.Initiate();
        return router;
    }
}
