// <copyright file="KafkaBrokerRoutesTopicsOffsetExtractor.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.DataPlatforms.Secu4.Routing.Configuration.Core;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;

namespace MA.Streaming.Core.Routing;

public class KafkaBrokerRoutesOffsetExtractor : IKafkaBrokerRoutesOffsetExtractor
{
    protected readonly IStreamingApiConfiguration Configuration;
    protected readonly IKafkaTopicHelper KafkaTopicHelper;
    protected readonly IKafkaRouteInfoCreator KafkaRouteInfoCreator;
    private readonly string brokerUrl;
    private readonly KafkaSecurityConfig? securityConfig;
    private readonly object readWriteLock = new();

    public KafkaBrokerRoutesOffsetExtractor(
        IKafkaTopicHelper kafkaTopicHelper,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IKafkaRouteInfoCreator kafkaRouteInfoCreator,
        IKafkaSecurityConfigProvider kafkaSecurityConfigProvider)
    {
        this.KafkaTopicHelper = kafkaTopicHelper;
        this.KafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.Configuration = streamingApiConfigurationProvider.Provide();
        this.brokerUrl = this.Configuration.BrokerUrl;
        this.securityConfig = kafkaSecurityConfigProvider.Provide();
    }

    public IReadOnlyList<TopicPartitionOffsetDto> Extract(string dataSource)
    {
        lock (this.readWriteLock)
        {
            var lstResult = new List<TopicPartitionOffsetDto>();
            var mainTopicRouteInfo = this.KafkaRouteInfoCreator.CreateMainRouteInfo(dataSource);
            var mainTopicName = mainTopicRouteInfo.KafkaRouteTopicInfo.Topic;

            var dataTopicInfos = this.KafkaTopicHelper.GetInfoByTopicPrefix(this.brokerUrl, mainTopicName, this.securityConfig);

            AddMainStreamTopicPartitionOffset(dataTopicInfos, mainTopicRouteInfo, lstResult);
            this.AddEssentialStreamTopicPartitionOffset(dataSource, lstResult);
            this.AddDataStreamTopicPartitionOffset(lstResult, dataTopicInfos, mainTopicName, dataSource);

            return lstResult;
        }
    }

    private void AddDataStreamTopicPartitionOffset(
        List<TopicPartitionOffsetDto> lstResult,
        IReadOnlyList<TopicInfo> dataTopicInfos,
        string mainTopicName,
        string dataSource)
    {
        if (this.Configuration.StreamCreationStrategy == StreamCreationStrategy.PartitionBased)
        {
            var validPartitionNumbers = (this.Configuration.PartitionMappings ?? []).Select(j => j.Partition).ToList();
            if (validPartitionNumbers.Count <= 0)
            {
                return;
            }

            var mainStreamTopicInfos = dataTopicInfos.Where(i => i.TopicName == mainTopicName).ToList();
            lstResult.AddRange(
                from validPartition in validPartitionNumbers
                let foundTopicOffsetInfo = mainStreamTopicInfos.FirstOrDefault(i => i.Partition == validPartition)
                select foundTopicOffsetInfo != null
                    ? new TopicPartitionOffsetDto(mainTopicName, foundTopicOffsetInfo.Partition, foundTopicOffsetInfo.Offset)
                    : new TopicPartitionOffsetDto(mainTopicName, validPartition, 0));
        }
        else
        {
            var streams = dataTopicInfos.Select(i => i.TopicName.Replace(mainTopicName, string.Empty)).Where(i => !string.IsNullOrEmpty(i)).Distinct().ToList();
            lstResult.AddRange(
                from stream in streams
                let streamOffsetInfo = dataTopicInfos.FirstOrDefault(i => i.TopicName.Replace(mainTopicName, string.Empty) == stream && i.Partition == 0)
                select streamOffsetInfo != null
                    ? new TopicPartitionOffsetDto(streamOffsetInfo.TopicName, streamOffsetInfo.Partition, streamOffsetInfo.Offset)
                    : new TopicPartitionOffsetDto(this.KafkaRouteInfoCreator.CreateStreamRouteInfo(dataSource, stream).KafkaRouteTopicInfo.Topic, 0, 0));
        }
    }

    private static void AddMainStreamTopicPartitionOffset(
        IEnumerable<TopicInfo> dataTopicInfos,
        KafkaRouteInfo mainTopicRouteInfo,
        List<TopicPartitionOffsetDto> lstResult)
    {
        var mainStreamOffsetInfo = dataTopicInfos.FirstOrDefault(i => i.TopicName == mainTopicRouteInfo.KafkaRouteTopicInfo.Topic && i.Partition == 0);
        lstResult.Add(
            mainStreamOffsetInfo != null
                ? new TopicPartitionOffsetDto(mainStreamOffsetInfo.TopicName, mainStreamOffsetInfo.Partition, mainStreamOffsetInfo.Offset)
                : new TopicPartitionOffsetDto(mainTopicRouteInfo.KafkaRouteTopicInfo.Topic, 0, 0));
    }

    private void AddEssentialStreamTopicPartitionOffset(string dataSource, List<TopicPartitionOffsetDto> lstResult)
    {
        var essentialRouteInfo = this.KafkaRouteInfoCreator.CreateEssentialRouteInfo(dataSource);
        var essentialTopicInfos = this.KafkaTopicHelper.GetInfoByTopicPrefix(this.brokerUrl, essentialRouteInfo.KafkaRouteTopicInfo.Topic, this.securityConfig);
        var essentialTopicInfo = essentialTopicInfos.FirstOrDefault(i => i.TopicName == essentialRouteInfo.KafkaRouteTopicInfo.Topic && i.Partition == 0);
        lstResult.Add(
            essentialTopicInfo != null
                ? new TopicPartitionOffsetDto(essentialTopicInfo.TopicName, essentialTopicInfo.Partition, essentialTopicInfo.Offset)
                : new TopicPartitionOffsetDto(essentialRouteInfo.KafkaRouteTopicInfo.Topic, 0, 0));
    }
}
