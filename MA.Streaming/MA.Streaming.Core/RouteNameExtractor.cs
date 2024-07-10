// <copyright file="RouteNameExtractor.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.Abstraction;

namespace MA.Streaming.Core;

public class RouteNameExtractor : IRouteNameExtractor
{
    private readonly IRouteBindingInfoRepository routeBindingInfoRepository;
    private readonly ILogger logger;

    public RouteNameExtractor(
        IRouteBindingInfoRepository routeBindingInfoRepository,
        ILogger logger)
    {
        this.routeBindingInfoRepository = routeBindingInfoRepository;
        this.logger = logger;
    }

    public string Extract(string dataSource, string stream = "")
    {
        if (string.IsNullOrEmpty(stream))
        {
            return this.GetDataSourceMainRoute(dataSource);
        }

        var streamBindingInfo = this.routeBindingInfoRepository.GetBindingInfoByDataSourceAndStream(dataSource, stream);
        return streamBindingInfo == null ? this.GetDataSourceMainRoute(dataSource) : streamBindingInfo.RouteName;
    }

    private string GetDataSourceMainRoute(string dataSource)
    {
        var dataSourceBindingInfo = this.routeBindingInfoRepository.GetBindingInfoByDataSourceAndStream(dataSource);
        if (dataSourceBindingInfo != null)
        {
            return dataSourceBindingInfo.RouteName;
        }

        this.logger.Error($"the route binding info not found for datasource:{dataSource}");
        return string.Empty;
    }
}
