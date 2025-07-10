// <copyright file="StreamsProvider.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing;

public class StreamsProvider : IStreamsProvider
{
    private readonly IKafkaTopicHelper kafkaTopicHelper;
    private readonly IStreamingApiConfiguration configuration;
    private List<string> lastStreams;
    private DateTime lastUpdateTime;

    public StreamsProvider(IStreamingApiConfigurationProvider apiConfigurationProvider, IKafkaTopicHelper kafkaTopicHelper)
    {
        this.kafkaTopicHelper = kafkaTopicHelper;
        this.configuration = apiConfigurationProvider.Provide();
        this.lastUpdateTime = DateTime.MinValue;
        this.lastStreams = [];
    }

    public IReadOnlyList<string> Provide(string dataSource)
    {
        return this.configuration.StreamCreationStrategy switch
        {
            StreamCreationStrategy.PartitionBased => this.configuration.PartitionMappings?.Select(i => i.Stream).ToList() ?? [],
            StreamCreationStrategy.TopicBased => this.GetStreamsFromTopic(dataSource),
            _ => []
        };
    }

    private IReadOnlyList<string> GetStreamsFromTopic(string dataSource)
    {
        if ((DateTime.UtcNow - this.lastUpdateTime).TotalSeconds < 1)
        {
            return this.lastStreams;
        }

        var topicInfos = this.kafkaTopicHelper.GetInfoByTopicPrefix(this.configuration.BrokerUrl, dataSource);
        var pattern = $@"^{dataSource}\.[^.]*$";
        var res = topicInfos.Where(i => Regex.Match(i.TopicName, pattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1)).Success)
            .Select(foundTopicInfo => foundTopicInfo.TopicName.Replace($"{dataSource}.", "")).ToList();
        this.lastUpdateTime = DateTime.UtcNow;
        this.lastStreams = res;
        return res;
    }
}
