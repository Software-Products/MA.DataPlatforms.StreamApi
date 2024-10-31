// <copyright file="KafkaRouteInfoProvider.cs" company="McLaren Applied Ltd.">
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

public abstract class KafkaRouteInfoProvider : IRouteInfoProvider
{
    protected readonly IStreamingApiConfiguration Configuration;
    protected readonly IKafkaTopicHelper KafkaTopicHelper;
    protected readonly IEssentialTopicNameCreator EssentialTopicNameCreator;

    protected KafkaRouteInfoProvider(
        IKafkaTopicHelper kafkaTopicHelper,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IEssentialTopicNameCreator essentialTopicNameCreator)
    {
        this.KafkaTopicHelper = kafkaTopicHelper;
        this.Configuration = streamingApiConfigurationProvider.Provide();
        this.EssentialTopicNameCreator = essentialTopicNameCreator;
    }

    public IReadOnlyList<IRouteInfo> GetRouteInfo(string dataSource)
    {
        var topicInfos = this.KafkaTopicHelper.GetInfoByTopicPrefix(this.Configuration.BrokerUrl, dataSource);
        return this.ExtractPartitionBasedRouteInfo(dataSource, topicInfos);
    }

    protected abstract IReadOnlyList<IRouteInfo> ExtractPartitionBasedRouteInfo(string dataSource, IReadOnlyList<TopicInfo> topicInfos);

    protected static string CreateRouteName(string dataSource, string stream)
    {
        return $"{dataSource}.{stream}";
    }

    protected KafkaRouteInfo CreateEssentialRouteInfo(string dataSource, IEnumerable<TopicInfo> topicInfos)
    {
        var essentialTopicName = this.EssentialTopicNameCreator.Create(dataSource);
        var essentialTopicInfo = topicInfos.FirstOrDefault(i => i.TopicName == essentialTopicName && i.Partition == 0);
        const string Stream = Constants.EssentialStreamName;
        return essentialTopicInfo == null
            ? new KafkaRouteInfo(CreateRouteName(dataSource, Stream), essentialTopicName, 0, 0, dataSource, Stream)
            : new KafkaRouteInfo(CreateRouteName(dataSource, Stream), essentialTopicName, 0, essentialTopicInfo.Offset, dataSource, Stream);
    }
}
