// <copyright file="DomainKafkaRouteMappingInfoCreator.cs" company="Motion Applied Ltd.">
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

public class DomainKafkaRouteMappingInfoCreator : IDomainKafkaRouteMappingInfoCreator
{
    private readonly string domain;
    private readonly StreamCreationStrategy strategy;
    private readonly string domainPrefix;
    private readonly List<string> validStreams;

    public DomainKafkaRouteMappingInfoCreator(IDomainNameProvider domainNameProvider, IStreamingApiConfigurationProvider apiConfigurationProvider)
    {
        this.domain = domainNameProvider.Provide();
        var streamingApiConfiguration = apiConfigurationProvider.Provide();
        this.strategy = streamingApiConfiguration.StreamCreationStrategy;
        this.validStreams = streamingApiConfiguration.PartitionMappings?.Select(i => i.Stream).ToList() ?? [];
        this.domainPrefix = string.IsNullOrEmpty(this.domain) ? "" : this.domain + ".";
    }

    public RouteMappingInfo CreateMainDataTopicName(string dataSource)
    {
        var mainDataTopicName = $"{this.domainPrefix}{ConstantNames.DataTopicIdentifier}.{dataSource}";
        var mainRouteNameInfo = new RouteNameInfo(this.domain, dataSource, ConstantSystemKeyStreamNames.MainStreamName, RouteType.Main);
        return new RouteMappingInfo(mainRouteNameInfo, mainDataTopicName);
    }

    public RouteMappingInfo CreateStreamDataTopicName(string dataSource, string stream)
    {
        var streamDataTopicName = this.strategy == StreamCreationStrategy.PartitionBased || string.IsNullOrEmpty(stream)
            ? $"{this.domainPrefix}{ConstantNames.DataTopicIdentifier}.{dataSource}"
            : $"{this.domainPrefix}{ConstantNames.DataTopicIdentifier}.{dataSource}.{stream}";
        var validStream = this.Validate(stream);
        var routeType = validStream == ConstantSystemKeyStreamNames.MainStreamName ? RouteType.Main : RouteType.Stream;
        var streamRouteNameInfo = new RouteNameInfo(this.domain, dataSource, validStream, routeType);
        return new RouteMappingInfo(streamRouteNameInfo, streamDataTopicName);
    }

    private string Validate(string stream)
    {
        if ((this.strategy == StreamCreationStrategy.PartitionBased && !this.validStreams.Contains(stream)) ||
            string.IsNullOrEmpty(stream))
        {
            return ConstantSystemKeyStreamNames.MainStreamName;
        }

        return stream;
    }

    public RouteMappingInfo CreateEssentialTopicName(string dataSource)
    {
        var essentialTopicName = $"{this.domainPrefix}{ConstantNames.EssentialsTopicIdentifier}.{dataSource}";
        var essentialRouteNameInfo = new RouteNameInfo(this.domain, dataSource, ConstantSystemKeyStreamNames.EssentialStreamName, RouteType.Essential);
        return new RouteMappingInfo(essentialRouteNameInfo, essentialTopicName);
    }

    public RouteMappingInfo CreateDeadLetterTopicName()
    {
        var deadLetterTopicName = $"{this.domainPrefix}{ConstantNames.SystemTopicIdentifier}.{ConstantNames.DeadLetterIdentifier}";
        var deadLetterRouteNameInfo = new RouteNameInfo(this.domain, "", ConstantSystemKeyStreamNames.DeadLetterStreamName, RouteType.DeadLetter);
        return new RouteMappingInfo(deadLetterRouteNameInfo, deadLetterTopicName);
    }

    public RouteMappingInfo CreateSessionInfoTopicName()
    {
        var sessionInfoTopicName = $"{this.domainPrefix}{ConstantNames.SystemTopicIdentifier}.{ConstantNames.SessionInfoIdentifier}";
        var sessionInfoRouteNameInfo = new RouteNameInfo(this.domain, "", ConstantSystemKeyStreamNames.SessionInfoStreamName, RouteType.SessionInfo);
        return new RouteMappingInfo(sessionInfoRouteNameInfo, sessionInfoTopicName);
    }

    public RouteMappingInfo CreateSystemStatusTopicName()
    {
        var systemStatusTopicName = $"{this.domainPrefix}{ConstantNames.SystemTopicIdentifier}.{ConstantNames.StatusIdentifier}";
        var systemStatusRouteNameInfo = new RouteNameInfo(this.domain, "", ConstantSystemKeyStreamNames.SystemStatusStreamName, RouteType.SystemStatus);
        return new RouteMappingInfo(systemStatusRouteNameInfo, systemStatusTopicName);
    }
}
