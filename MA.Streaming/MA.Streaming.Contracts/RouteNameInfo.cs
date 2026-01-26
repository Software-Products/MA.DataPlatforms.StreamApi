// <copyright file="RouteNameInfo.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Contracts;

public record RouteNameInfo
{
    public string Domain { get; }

    public string DataSource { get; }

    public string Stream { get; }

    public RouteType RouteType { get; }

    public RouteNameInfo(string domain, string dataSource, string stream, RouteType routeType)

    {
        this.Domain = domain;
        this.DataSource = dataSource;
        this.Stream = stream;
        this.RouteType = routeType;
        this.Name = string.IsNullOrEmpty(dataSource) ? $"{domain}.{stream}.{routeType}" : $"{domain}.{dataSource}.{stream}.{routeType}";
    }

    public string Name { get; }

    public override string ToString()
    {
        return this.Name;
    }
}
