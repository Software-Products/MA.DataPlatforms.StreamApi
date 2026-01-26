// <copyright file="RoutingMappingInfoRepository.cs" company="Motion Applied Ltd.">
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

using MA.Streaming.Abstraction;

namespace MA.Streaming.Core.Routing;

public class RoutingMappingInfoRepository : IRoutingMappingInfoRepository
{
    private readonly object lockObj = new();

    private readonly Dictionary<string, RouteMappingInfo> routeInfos = new();

    public void AddRouteInfo(RouteMappingInfo routeInfo)
    {
        lock (this.lockObj)
        {
            if (!this.routeInfos.TryGetValue(routeInfo.RouteNameInfo.Name, out var value))
            {
                this.routeInfos.Add(routeInfo.RouteNameInfo.Name, routeInfo);
            }
        }
    }

    public RouteMappingInfo? GetRouteInfo(string routeName)
    {
        lock (this.lockObj)
        {
            return this.routeInfos.GetValueOrDefault(routeName);
        }
    }
}
