// <copyright file="KafkaRouteInfoCreator.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Core.Routing;

public class KafkaRouteInfoCreator : IKafkaRouteInfoCreator
{
    private readonly IDomainKafkaRouteMappingInfoCreator domainKafkaRouteMappingInfoCreator;
    private readonly IRoutingMappingInfoRepository routingMappingInfoRepository;
    private readonly IStreamingApiConfiguration configuration;
    private readonly object lockObject = new();

    public KafkaRouteInfoCreator(
        IDomainKafkaRouteMappingInfoCreator domainKafkaRouteMappingInfoCreator,
        IStreamingApiConfigurationProvider apiConfigurationProvider,
        IRoutingMappingInfoRepository routingMappingInfoRepository)
    {
        this.domainKafkaRouteMappingInfoCreator = domainKafkaRouteMappingInfoCreator;
        this.routingMappingInfoRepository = routingMappingInfoRepository;
        this.configuration = apiConfigurationProvider.Provide();
    }

    private KafkaRouteInfo CreateRouteInfo(RouteMappingInfo mappingInfo, int partition)
    {
        lock (this.lockObject)
        {
            this.routingMappingInfoRepository.AddRouteInfo(mappingInfo);
        }

        return new KafkaRouteInfo(mappingInfo.RouteNameInfo, new KafkaRouteTopicInfo(mappingInfo.Topic, partition));
    }

    public KafkaRouteInfo CreateMainRouteInfo(string dataSource)
    {
        var mappingInfo = this.domainKafkaRouteMappingInfoCreator.CreateMainDataTopicName(dataSource);
        return this.CreateRouteInfo(mappingInfo, 0);
    }

    public KafkaRouteInfo CreateStreamRouteInfo(string dataSource, string stream)
    {
        var mappingInfo = this.domainKafkaRouteMappingInfoCreator.CreateStreamDataTopicName(dataSource, stream);
        var partition = this.ExtractPartition(mappingInfo.RouteNameInfo.Stream);
        return partition == null
            ? this.CreateMainRouteInfo(dataSource)
            : this.CreateRouteInfo(mappingInfo, partition.Value);
    }

    private int? ExtractPartition(string stream)
    {
        if (this.configuration.StreamCreationStrategy == StreamCreationStrategy.TopicBased)
        {
            return 0;
        }

        var partitionMapping = this.configuration.PartitionMappings?.FirstOrDefault(i => i.Stream == stream);

        return partitionMapping?.Partition;
    }

    public KafkaRouteInfo CreateEssentialRouteInfo(string dataSource)
    {
        var mappingInfo = this.domainKafkaRouteMappingInfoCreator.CreateEssentialTopicName(dataSource);
        return this.CreateRouteInfo(mappingInfo, 0);
    }

    public KafkaRouteInfo CreateDeadLetterRouteInfo()
    {
        var mappingInfo = this.domainKafkaRouteMappingInfoCreator.CreateDeadLetterTopicName();
        return this.CreateRouteInfo(mappingInfo, 0);
    }

    public KafkaRouteInfo CreateSessionInfoRouteInfo()
    {
        var mappingInfo = this.domainKafkaRouteMappingInfoCreator.CreateSessionInfoTopicName();
        return this.CreateRouteInfo(mappingInfo, 0);
    }

    public KafkaRouteInfo CreateSystemStatusRouteInfo()
    {
        var mappingInfo = this.domainKafkaRouteMappingInfoCreator.CreateSystemStatusTopicName();
        return this.CreateRouteInfo(mappingInfo, 0);
    }
}
