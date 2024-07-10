// <copyright file="PartitionBasedKafkaRouteInfoProvider.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatform.Secu4.KafkaMetadataComponent;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;

namespace MA.Streaming.Core.Routing;

public class PartitionBasedKafkaRouteInfoProvider : KafkaRouteInfoProvider
{
    public PartitionBasedKafkaRouteInfoProvider(IKafkaTopicHelper kafkaTopicHelper, IStreamingApiConfigurationProvider streamingApiConfigurationProvider)
        : base(kafkaTopicHelper, streamingApiConfigurationProvider)
    {
    }

    protected override IReadOnlyList<IRouteInfo> ExtractPartitionBasedRouteInfo(string dataSource, IReadOnlyList<TopicInfo> topicInfos)
    {
        var configurationPartitionMappings = this.Configuration.PartitionMappings ?? [];
        return topicInfos.Any()
            ? ExtractRouteInfos(dataSource, topicInfos, configurationPartitionMappings)
            : ReturnDefaultRouteInfos(dataSource, configurationPartitionMappings);
    }

    private static IReadOnlyList<IRouteInfo> ExtractRouteInfos(
        string dataSource,
        IReadOnlyList<TopicInfo> topicInfos,
        IEnumerable<PartitionMapping> configurationPartitionMappings)
    {
        var mainRouteInfo = CreateKafkaRouteInfo(dataSource, topicInfos);
        var result = new List<KafkaRouteInfo>
        {
            mainRouteInfo
        };
        result.AddRange(
            configurationPartitionMappings.Select(
                configurationPartitionMapping => CreateKafkaRouteInfo(dataSource, topicInfos, configurationPartitionMapping)));

        return result;
    }

    private static KafkaRouteInfo CreateKafkaRouteInfo(
        string dataSource,
        IEnumerable<TopicInfo> topicInfos,
        PartitionMapping? partitionMapping = null)
    {
        var partitionNumber = (partitionMapping?.Partition ?? 0);
        var topicInfo = topicInfos.FirstOrDefault(i => i.Partition == partitionNumber);
        var offset = topicInfo?.Offset ?? 0;
        var routeInfo = new KafkaRouteInfo(dataSource, dataSource, partitionNumber, offset, dataSource, partitionMapping?.Stream ?? "");
        return routeInfo;
    }

    private static IReadOnlyList<IRouteInfo> ReturnDefaultRouteInfos(string dataSource, IEnumerable<PartitionMapping> configurationPartitionMappings)
    {
        var result = new List<KafkaRouteInfo>
        {
            new(dataSource, dataSource, 0, 0, dataSource)
        };
        result.AddRange(configurationPartitionMappings.Select(i => new KafkaRouteInfo($"{dataSource}.{i.Stream}", dataSource, i.Partition, 0, dataSource, i.Stream)));

        return result;
    }
}
