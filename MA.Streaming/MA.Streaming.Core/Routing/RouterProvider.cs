// <copyright file="TopicBasedRouterProvider.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing;

public class RouterProvider : IRouterProvider
{
    private readonly IDomainRouterProvider domainRouterProvider;
    private readonly IEssentialRouterProvider essentialRouterProvider;
    private readonly IDataRouterProvider dataRouterProvider;

    public RouterProvider(
        IDomainRouterProvider domainRouterProvider,
        IEssentialRouterProvider essentialRouterProvider,
        IDataRouterProvider dataRouterProvider)
    {
        this.domainRouterProvider = domainRouterProvider;
        this.essentialRouterProvider = essentialRouterProvider;
        this.dataRouterProvider = dataRouterProvider;
    }

    public IRouter ProvideMainStreamRouter(string dataSource)
    {
        return this.dataRouterProvider.ProvideMainStream(dataSource);
    }

    public IRouter ProvideDataStreamRouter(string dataSource, string stream)
    {
        return this.dataRouterProvider.ProvideDataStream(dataSource, stream);
    }

    public IRouter ProvideEssentialRouter(string dataSource)
    {
        return this.essentialRouterProvider.Provide(dataSource);
    }

    public IRouter ProvideSessionInfoRouter()
    {
        return this.domainRouterProvider.Provide();
    }

    public IRouter ProvideSystemStatusRouter()
    {
        return this.domainRouterProvider.Provide();
    }

    public IRouter ProvideDeadLetterRouter()
    {
        return this.domainRouterProvider.Provide();
    }
}
