// <copyright file="StreamsProvider.cs" company="Motion Applied Ltd.">
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

using System.Text.RegularExpressions;

using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.DataPlatforms.Secu4.Routing.Configuration.Core;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing;

public class StreamsProvider : IStreamsProvider
{
    private readonly IKafkaTopicHelper kafkaTopicHelper;
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IStreamingApiConfiguration configuration;
    private readonly KafkaSecurityConfig? securityConfig;
    private List<string> lastStreams;
    private DateTime lastUpdateTime;

    public StreamsProvider(IStreamingApiConfigurationProvider apiConfigurationProvider, IKafkaTopicHelper kafkaTopicHelper, IKafkaRouteInfoCreator kafkaRouteInfoCreator, IKafkaSecurityConfigProvider kafkaSecurityConfigProvider)
    {
        this.kafkaTopicHelper = kafkaTopicHelper;
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.configuration = apiConfigurationProvider.Provide();
        this.securityConfig = kafkaSecurityConfigProvider.Provide();
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

    private List<string> GetStreamsFromTopic(string dataSource)
    {
        if ((DateTime.UtcNow - this.lastUpdateTime).TotalSeconds < 1)
        {
            return this.lastStreams;
        }

        var mainDataSourceRouteInfo = this.kafkaRouteInfoCreator.CreateMainRouteInfo(dataSource);
        var topicInfos = this.kafkaTopicHelper.GetInfoByTopicPrefix(this.configuration.BrokerUrl, mainDataSourceRouteInfo.KafkaRouteTopicInfo.Topic, this.securityConfig);
        var pattern = $@"^{mainDataSourceRouteInfo.KafkaRouteTopicInfo.Topic}\.(.+)$";
        var res = topicInfos.Where(i => Regex.Match(i.TopicName, pattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1)).Success)
            .Select(foundTopicInfo => foundTopicInfo.TopicName.Replace($"{mainDataSourceRouteInfo.KafkaRouteTopicInfo.Topic}.", ""))
            .Where(i => !string.IsNullOrWhiteSpace(i)).Distinct().ToList();
        this.lastUpdateTime = DateTime.UtcNow;
        this.lastStreams = res;
        return res;
    }
}
