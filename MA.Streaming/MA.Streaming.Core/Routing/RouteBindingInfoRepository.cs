// <copyright file="RouteBindingInfoRepository.cs" company="McLaren Applied Ltd.">
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

using System.Collections.Concurrent;

using MA.Streaming.Abstraction;

namespace MA.Streaming.Core.Routing;

public class RouteBindingInfoRepository : IRouteBindingInfoRepository
{
    private readonly ConcurrentDictionary<string, RouteBindingInfo> routesBasedDictionary = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, RouteBindingInfo>> streamBasedDictionary = new();
    private readonly ConcurrentDictionary<string, RouteBindingInfo> dataSourceBasedDictionary = new();

    public RouteBindingInfo? GetBindingInfoByDataSourceAndStream(string dataSource, string stream = "")
    {
        if (string.IsNullOrEmpty(stream))
        {
            this.dataSourceBasedDictionary.TryGetValue(dataSource, out var dataSourceBindingInfo);
            return dataSourceBindingInfo;
        }
        else
        {
            this.streamBasedDictionary.TryGetValue(dataSource, out var dataSourceStreamsDic);
            if (dataSourceStreamsDic == null)
            {
                return null;
            }

            dataSourceStreamsDic.TryGetValue(stream, out var bindingInfo);
            return bindingInfo;
        }
    }

    public RouteBindingInfo? GetBindingInfoByRoute(string routeName)
    {
        this.routesBasedDictionary.TryGetValue(routeName, out var bindingInfo);
        return bindingInfo;
    }

    public RouteBindingInfo GetOrAdd(string dataSource, string stream = "", bool isEssential = false)
    {
        var routeBindingInfo = this.GetBindingInfoByDataSourceAndStream(dataSource, stream);
        if (routeBindingInfo != null)
        {
            return routeBindingInfo;
        }

        var routeName = Guid.NewGuid().ToString();
        var newRouteBindingInfo = new RouteBindingInfo(routeName, dataSource, stream, isEssential);
        var savedBindingInfo = this.routesBasedDictionary.AddOrUpdate(routeName, newRouteBindingInfo, (_, existBindingInfo) => existBindingInfo);
        if (string.IsNullOrEmpty(stream))
        {
            return this.dataSourceBasedDictionary.AddOrUpdate(dataSource, savedBindingInfo, (_, existBindingInfo) => existBindingInfo);
        }

        this.streamBasedDictionary.TryGetValue(dataSource, out var dataSourceStreamsDic);
        if (dataSourceStreamsDic != null)
        {
            return dataSourceStreamsDic.AddOrUpdate(stream, savedBindingInfo, (_, existBindingInfo) => existBindingInfo);
        }

        var savedDictionary = this.streamBasedDictionary.AddOrUpdate(
            dataSource,
            new ConcurrentDictionary<string, RouteBindingInfo>(),
            (_, existDic) => existDic);
        return savedDictionary.AddOrUpdate(stream, savedBindingInfo, (_, existBindingInfo) => existBindingInfo);
    }
}
