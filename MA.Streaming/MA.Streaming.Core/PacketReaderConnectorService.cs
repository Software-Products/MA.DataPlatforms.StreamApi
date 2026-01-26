// <copyright file="PacketReaderConnectorService.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.PrometheusMetrics;

namespace MA.Streaming.Core;

public class PacketReaderConnectorService : IPacketReaderConnectorService
{
    private readonly IRouteSubscriber routeSubscriber;
    private readonly ILogger logger;
    private readonly IRoutingMappingInfoRepository routingMappingInfoRepository;
    private readonly string sessionKey;
    private readonly KafkaRouteInfo mainRouteInfo;

    public PacketReaderConnectorService(
        ConnectionDetailsDto connectionDetailsDto,
        IRouteSubscriber routeSubscriber,
        ILogger logger,
        IRoutingMappingInfoRepository routingMappingInfoRepository,
        IKafkaRouteInfoCreator kafkaRouteInfoCreator)
    {
        this.routeSubscriber = routeSubscriber;
        this.logger = logger;
        this.routingMappingInfoRepository = routingMappingInfoRepository;
        this.ConnectionId = connectionDetailsDto.Id;
        this.sessionKey = connectionDetailsDto.SessionKey;
        this.mainRouteInfo = kafkaRouteInfoCreator.CreateMainRouteInfo(connectionDetailsDto.DataSource);
    }

    public event EventHandler<PacketReceivedInfoEventArgs>? PacketReceived;

    public long ConnectionId { get; }

    public void Start()
    {
        this.routeSubscriber.PacketReceived += this.Subscriber_PacketReceived;
        this.routeSubscriber.Subscribe();
    }

    public void Stop()
    {
        try
        {
            this.logger.Info($"stopping packet reader {this.sessionKey}-{this.ConnectionId}");
            this.routeSubscriber.PacketReceived -= this.Subscriber_PacketReceived;
            this.routeSubscriber.Unsubscribe();
            this.logger.Info($"packet reader {this.sessionKey}-{this.ConnectionId} stopped");
        }
        catch (Exception ex)
        {
            this.logger.Error($"Exception happened on stopping the reader connector service subscriber {ex}");
        }
    }

    private void Subscriber_PacketReceived(object? sender, RoutingDataPacket e)
    {
        if (!string.IsNullOrEmpty(this.sessionKey) &&
            e.Key != this.sessionKey)
        {
            return;
        }

        var routeMappingInfo = this.routingMappingInfoRepository.GetRouteInfo(e.Route);

        if (routeMappingInfo == null)
        {
            this.logger.Error($"binding info for route{e.Route} is not found while listening to the route {e.Route}");
            return;
        }

        var messageStreamName = (routeMappingInfo.RouteNameInfo.Stream == this.mainRouteInfo.RouteNameInfo.Stream)
            ? string.Empty
            : routeMappingInfo.RouteNameInfo.Stream;

        MetricProviders.NumberOfReceivedMessagesFromRouter.WithLabels(
                this.ConnectionId.ToString(),
                routeMappingInfo.RouteNameInfo.DataSource,
                routeMappingInfo.RouteNameInfo.Stream,
                e.Key ?? string.Empty)
            .Inc();
        MetricProviders.NumberOfReceivedBytesFromRouter.WithLabels(
                this.ConnectionId.ToString(),
                routeMappingInfo.RouteNameInfo.DataSource,
                routeMappingInfo.RouteNameInfo.Stream,
                e.Key ?? string.Empty)
            .Inc(e.Message.Length);

        this.PacketReceived?.Invoke(
            this,
            new PacketReceivedInfoEventArgs(
                this.ConnectionId,
                routeMappingInfo.RouteNameInfo.DataSource,
                messageStreamName,
                e.Key ?? string.Empty,
                e.SubmitTime,
                e.Message));
    }
}
