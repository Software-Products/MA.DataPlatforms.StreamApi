// <copyright file="TopicBasedRoutingConfigurationProvider.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;

namespace MA.Streaming.Core.Routing.TopicsRouting;

public class TopicBasedRoutingConfigurationProvider : IRoutingConfigurationProvider
{
    private readonly string dataSource;
    private readonly string stream;
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly IRouteBindingInfoRepository routeBindingInfoRepository;
    private readonly ITopicBaseTopicNameCreator topicBaseTopicNameCreator;

    public TopicBasedRoutingConfigurationProvider(
        string dataSource,
        string stream,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IRouteBindingInfoRepository routeBindingInfoRepository,
        ITopicBaseTopicNameCreator topicBaseTopicNameCreator)
    {
        this.dataSource = dataSource;
        this.stream = stream;
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.routeBindingInfoRepository = routeBindingInfoRepository;
        this.topicBaseTopicNameCreator = topicBaseTopicNameCreator;
    }

    public RoutingConfiguration Provide()
    {
        var bindingInfo = this.routeBindingInfoRepository.GetOrAdd(this.dataSource, this.stream);
        var topicName = this.topicBaseTopicNameCreator.Create(this.dataSource, this.stream);
        var routes = new List<KafkaRoute>
        {
            new(bindingInfo.RouteName, topicName, 0)
        };
        var config = this.streamingApiConfigurationProvider.Provide();
        return new RoutingConfiguration(
            new KafkaRoutingConfig(
                new KafkaPublishingConfig(server: config.BrokerUrl, compressionLevel: -1, compressionType: 4, 0, 1_000_000, 500_000_000),
                routes,
                [new KafkaTopicMetaData(topicName)],
                "dead-Letter"));
    }
}
