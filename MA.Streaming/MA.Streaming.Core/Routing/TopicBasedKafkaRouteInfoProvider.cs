// <copyright file="TopicBasedKafkaRouteInfoProvider.cs" company="McLaren Applied Ltd.">
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

using System.Text.RegularExpressions;

using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Routing.EssentialsRouting;

namespace MA.Streaming.Core.Routing;

public class TopicBasedKafkaRouteInfoProvider : KafkaRouteInfoProvider
{
    public TopicBasedKafkaRouteInfoProvider(
        IKafkaTopicHelper kafkaTopicHelper,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IEssentialTopicNameCreator essentialTopicNameCreator)
        : base(kafkaTopicHelper, streamingApiConfigurationProvider, essentialTopicNameCreator)
    {
    }

    protected override IReadOnlyList<IRouteInfo> ExtractPartitionBasedRouteInfo(string dataSource, IReadOnlyList<TopicInfo> topicInfos)
    {
        var result = new List<IRouteInfo>();
        var mainTopicInfo = topicInfos.FirstOrDefault(i => i.TopicName == dataSource && i.Partition == 0);
        result.Add(CreateMainRouteInfo(dataSource, mainTopicInfo));
        result.Add(this.CreateEssentialRouteInfo(dataSource, topicInfos));
        result.AddRange(
            CreateStreamsRouteInfo(dataSource, topicInfos));

        return result;
    }

    private static IEnumerable<KafkaRouteInfo> CreateStreamsRouteInfo(string dataSource, IEnumerable<TopicInfo> topicInfos)
    {
        var pattern = $@"^{dataSource}\.[^.]*$";
        return (from foundTopicInfo in topicInfos.Where(i => Regex.Match(i.TopicName, pattern, RegexOptions.IgnoreCase,TimeSpan.FromSeconds(1)).Success)
            let streamName = foundTopicInfo.TopicName.Replace($"{dataSource}.", "")
            select new KafkaRouteInfo($"{dataSource}.{streamName}", foundTopicInfo.TopicName, 0, foundTopicInfo.Offset, dataSource, streamName));
    }

    private static KafkaRouteInfo CreateMainRouteInfo(string dataSource, TopicInfo? mainTopicInfo)
    {
        return new KafkaRouteInfo(dataSource, dataSource, 0, mainTopicInfo?.Offset ?? 0, dataSource);
    }
}
