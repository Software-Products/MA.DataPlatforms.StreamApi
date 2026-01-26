// <copyright file="EssentialRouterProvider.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Core.Routing.EssentialsRouting;

public class EssentialRouterProvider : IEssentialRouterProvider
{
    private readonly ILogger logger;
    private readonly IEssentialRouterConfigurationProvider essentialRouterConfigurationProvider;
    private readonly IRouteManager routeManager;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouter> readingWritingComponentRepository;
    private readonly object readWriteLock = new();
    private readonly Dictionary<string, IRouter> essentialRouters = new();

    public EssentialRouterProvider(
        ILogger logger,
        IEssentialRouterConfigurationProvider essentialRouterConfigurationProvider,
        IRouteManager routeManager,
        IRouteReadingWritingComponentRepository<KafkaRouter> readingWritingComponentRepository)
    {
        this.logger = logger;
        this.essentialRouterConfigurationProvider = essentialRouterConfigurationProvider;
        this.routeManager = routeManager;
        this.readingWritingComponentRepository = readingWritingComponentRepository;
    }

    public IRouter Provide(string dataSource)
    {
        lock (this.readWriteLock)
        {
            if (this.essentialRouters.TryGetValue(dataSource, out var existingRouter))
            {
                return existingRouter;
            }

            var configurationProvider = this.essentialRouterConfigurationProvider.Provide(dataSource);
            existingRouter = new KafkaRouter(
                this.logger,
                new KafkaProducerBuilder(
                    this.logger,
                    configurationProvider,
                    this.routeManager),
                this.readingWritingComponentRepository,
                $"Essential_Router_{dataSource}_{DateTime.Now.Ticks}");
            existingRouter.Initiate();
            this.essentialRouters[dataSource] = existingRouter;
            return existingRouter;
        }
    }
}
