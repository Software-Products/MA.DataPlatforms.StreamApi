// <copyright file="DataRouterProvider.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing.PartitionsRouting;
using MA.Streaming.Core.Routing.TopicsRouting;

namespace MA.Streaming.Core.Routing;

public class DataRouterProvider : IDataRouterProvider
{
    private readonly ILogger logger;
    private readonly IRoutingConfigurationProviderCreator routingConfigurationProviderCreator;

    private readonly IRouteManager routeManager;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouter> readingWritingComponentRepository;
    private readonly object mainDataReadWriteLock = new();
    private readonly object streamDataReadWriteLock = new();

    private readonly Dictionary<string, IRouter> mainStreamRouters = new();

    private readonly IRouterCreator routerCreator;

    public DataRouterProvider(
        ILogger logger,
        IRoutingConfigurationProviderCreator routingConfigurationProviderCreator,
        IRouteManager routeManager,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IRouteReadingWritingComponentRepository<KafkaRouter> readingWritingComponentRepository)
    {
        this.logger = logger;
        this.routingConfigurationProviderCreator = routingConfigurationProviderCreator;
        this.routeManager = routeManager;
        this.readingWritingComponentRepository = readingWritingComponentRepository;
        var config = streamingApiConfigurationProvider.Provide();

        if (config.StreamCreationStrategy == StreamCreationStrategy.PartitionBased)
        {
            this.routerCreator = new PartitionBasedRouterCreator(
                this.logger,
                this.routingConfigurationProviderCreator,
                this.routeManager,
                readingWritingComponentRepository);
        }
        else
        {
            this.routerCreator = new TopicBasedRouterCreator(
                this.logger,
                this.routingConfigurationProviderCreator,
                this.routeManager,
                readingWritingComponentRepository);
        }
    }

    public IRouter ProvideMainStream(string dataSource)
    {
        lock (this.mainDataReadWriteLock)
        {
            if (this.mainStreamRouters.TryGetValue(dataSource, out var existingRouter))
            {
                return existingRouter;
            }

            var configurationProvider = this.routingConfigurationProviderCreator.CreateMainStreamProvider(dataSource);

            existingRouter = new KafkaRouter(
                this.logger,
                new KafkaProducerBuilder(
                    this.logger,
                    configurationProvider,
                    this.routeManager),
                this.readingWritingComponentRepository,
                $"Data_Router_Main_{dataSource}_{DateTime.Now.Ticks}");
            existingRouter.Initiate();
            this.mainStreamRouters[dataSource] = existingRouter;
            return existingRouter;
        }
    }

    public IRouter ProvideDataStream(string dataSource, string stream)
    {
        lock (this.streamDataReadWriteLock)
        {
            return stream == ConstantSystemKeyStreamNames.MainStreamName ? this.ProvideMainStream(dataSource) : this.routerCreator.CreateRouter(dataSource, stream);
        }
    }
}
