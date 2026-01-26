// <copyright file="PartitionBasedRouterCreator.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing.PartitionsRouting;

internal class PartitionBasedRouterCreator : IRouterCreator
{
    private readonly ILogger logger;
    private readonly IRoutingConfigurationProviderCreator routingConfigurationProviderCreator;
    private readonly IRouteManager routeManager;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouter> readingWritingComponentRepository;
    private readonly Dictionary<string, IRouter> partitionBasedDataStreamRouters = new();

    public PartitionBasedRouterCreator(
        ILogger logger,
        IRoutingConfigurationProviderCreator routingConfigurationProviderCreator,
        IRouteManager routeManager,
        IRouteReadingWritingComponentRepository<KafkaRouter> readingWritingComponentRepository)
    {
        this.logger = logger;
        this.routingConfigurationProviderCreator = routingConfigurationProviderCreator;
        this.routeManager = routeManager;
        this.readingWritingComponentRepository = readingWritingComponentRepository;
    }

    public IRouter CreateRouter(string dataSource, string stream)
    {
        if (this.partitionBasedDataStreamRouters.TryGetValue(dataSource, out var existingRouter))
        {
            return existingRouter;
        }

        var configurationProvider = this.routingConfigurationProviderCreator.CreateDataStreamProvider(dataSource, stream);

        existingRouter = new KafkaRouter(
            this.logger,
            new KafkaProducerBuilder(
                this.logger,
                configurationProvider,
                this.routeManager),
            this.readingWritingComponentRepository,
            $"Data_Stream_Router_{dataSource}_{stream}_{DateTime.Now.Ticks}");
        existingRouter.Initiate();
        this.partitionBasedDataStreamRouters[dataSource] = existingRouter;
        return existingRouter;
    }
}
