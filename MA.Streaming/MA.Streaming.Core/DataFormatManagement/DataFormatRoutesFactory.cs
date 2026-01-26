// <copyright file="DataFormatRoutesFactory.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.DataFormatManagement;

public class DataFormatRoutesFactory : IDataFormatRoutesFactory
{
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;

    public DataFormatRoutesFactory(IKafkaRouteInfoCreator kafkaRouteInfoCreator)
    {
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
    }

    public IReadOnlyList<KafkaRoute> Create(IReadOnlyList<string> dataSources)
    {
        return dataSources
            .Select(dataSource => this.kafkaRouteInfoCreator.CreateEssentialRouteInfo(dataSource))
            .Select(kafkaRouteInfo => new KafkaRoute(
                kafkaRouteInfo.RouteNameInfo.Name,
                kafkaRouteInfo.KafkaRouteTopicInfo.Topic,
                kafkaRouteInfo.KafkaRouteTopicInfo.Partition)).ToList();
    }
}
