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

using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Routing.EssentialsRouting;

namespace MA.Streaming.Core.Routing;

public class PartitionBasedKafkaRouteInfoProvider : KafkaRouteInfoProvider
{
    public PartitionBasedKafkaRouteInfoProvider(
        IKafkaTopicHelper kafkaTopicHelper,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IEssentialTopicNameCreator essentialTopicNameCreator)
        : base(kafkaTopicHelper, streamingApiConfigurationProvider, essentialTopicNameCreator)
    {
    }

    protected override IReadOnlyList<IRouteInfo> ExtractPartitionBasedRouteInfo(string dataSource, IReadOnlyList<TopicInfo> topicInfos)
    {
        var configurationPartitionMappings = this.Configuration.PartitionMappings ?? [];
        return topicInfos.Any()
            ? this.ExtractRouteInfos(dataSource, topicInfos, configurationPartitionMappings)
            : this.ReturnDefaultRouteInfos(dataSource, configurationPartitionMappings);
    }

    private IReadOnlyList<IRouteInfo> ExtractRouteInfos(
        string dataSource,
        IReadOnlyList<TopicInfo> topicInfos,
        IEnumerable<PartitionMapping> configurationPartitionMappings)
    {
        var mainRouteInfo = CreateMainRouteInfo(dataSource, topicInfos);
        var essentialRoueInfo = this.CreateEssentialRouteInfo(dataSource, topicInfos);
        var result = new List<KafkaRouteInfo>
        {
            mainRouteInfo,
            essentialRoueInfo,
        };
        result.AddRange(
            configurationPartitionMappings.Select(
                configurationPartitionMapping => CreateStreamsKafkaRouteInfo(dataSource, topicInfos, configurationPartitionMapping)));

        return result;
    }

    private static KafkaRouteInfo CreateMainRouteInfo(string dataSource, IEnumerable<TopicInfo> topicInfos)
    {
        var mainTopicInfo = topicInfos.FirstOrDefault(i => i.TopicName == dataSource && i.Partition == 0);
        return mainTopicInfo == null
            ? new KafkaRouteInfo(CreateRouteName(dataSource, string.Empty), dataSource, 0, 0, dataSource)
            : new KafkaRouteInfo(CreateRouteName(dataSource, string.Empty), dataSource, 0, mainTopicInfo.Offset, dataSource);
    }

    private static KafkaRouteInfo CreateStreamsKafkaRouteInfo(
        string dataSource,
        IEnumerable<TopicInfo> topicInfos,
        PartitionMapping partitionMapping)
    {
        var topicInfo = topicInfos.FirstOrDefault(i => i.TopicName == dataSource && i.Partition == partitionMapping.Partition);
        var offset = topicInfo?.Offset ?? 0;
        var stream = partitionMapping.Stream;
        var routeInfo = new KafkaRouteInfo(CreateRouteName(dataSource, stream), dataSource, partitionMapping.Partition, offset, dataSource, stream);
        return routeInfo;
    }

    private IReadOnlyList<IRouteInfo> ReturnDefaultRouteInfos(string dataSource, IEnumerable<PartitionMapping> configurationPartitionMappings)
    {
        var result = new List<KafkaRouteInfo>
        {
            new(dataSource, dataSource, 0, 0, dataSource),
            this.CreateEssentialRouteInfo(dataSource, []),
        };
        result.AddRange(
            configurationPartitionMappings.Select(i => new KafkaRouteInfo(CreateRouteName(dataSource, i.Stream), dataSource, i.Partition, 0, dataSource, i.Stream)));

        return result;
    }
}
