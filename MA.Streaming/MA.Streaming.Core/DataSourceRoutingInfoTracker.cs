// <copyright file="DataSourceRoutingInfoTracker.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core;

public class DataSourceRoutingInfoTracker : IDataSourceRoutingInfoTracker
{
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;

    private readonly IDataSourcesRepository dataSourcesRepository;

    private readonly Dictionary<(string DataSource, string Stream), KafkaRouteInfo> dataTopicMappings = new();

    private readonly Dictionary<string, KafkaRouteInfo> essentialTopicMappings = new();

    private readonly KafkaRouteInfo sessionInfoRouteInfo;

    public DataSourceRoutingInfoTracker(IKafkaRouteInfoCreator kafkaRouteInfoCreator, IDataSourcesRepository dataSourcesRepository)
    {
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.dataSourcesRepository = dataSourcesRepository;
        this.sessionInfoRouteInfo = this.kafkaRouteInfoCreator.CreateSessionInfoRouteInfo();
    }

    public KafkaRouteInfo GetDataRouteInfo(string dataSource, string stream)
    {
        if (this.dataTopicMappings.TryGetValue((dataSource, stream), out var kafkaRouteInfo))
        {
            return kafkaRouteInfo;
        }

        this.dataSourcesRepository.Add(dataSource);
        var dataRouteInfo = this.kafkaRouteInfoCreator.CreateStreamRouteInfo(dataSource, stream);
        this.dataTopicMappings[(dataSource, stream)] = dataRouteInfo;
        return dataRouteInfo;
    }

    public KafkaRouteInfo GetEssentialRouteInfo(string dataSource)
    {
        if (this.essentialTopicMappings.TryGetValue(dataSource, out var topic))
        {
            return topic;
        }

        this.dataSourcesRepository.Add(dataSource);
        var essentialRouteInfo = this.kafkaRouteInfoCreator.CreateEssentialRouteInfo(dataSource);
        this.essentialTopicMappings[dataSource] = essentialRouteInfo;
        return essentialRouteInfo;
    }

    public KafkaRouteInfo GetSessionInfoRouteInfo(string dataSource)
    {
        this.dataSourcesRepository.Add(dataSource);
        return this.sessionInfoRouteInfo;
    }
}
