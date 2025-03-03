// <copyright file="PacketReaderConnectorService.cs" company="McLaren Applied Ltd.">
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
    private readonly IRouteBindingInfoRepository bindingInfoRepository;
    private readonly string sessionKey;

    public PacketReaderConnectorService(
        ConnectionDetailsDto connectionDetailsDto,
        IRouteSubscriber routeSubscriber,
        ILogger logger,
        IRouteBindingInfoRepository bindingInfoRepository)
    {
        this.routeSubscriber = routeSubscriber;
        this.logger = logger;
        this.bindingInfoRepository = bindingInfoRepository;
        this.ConnectionId = connectionDetailsDto.Id;
        this.sessionKey = connectionDetailsDto.SessionKey;
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
            this.routeSubscriber.PacketReceived -= this.Subscriber_PacketReceived;
            this.routeSubscriber.Unsubscribe();
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

        var bindingInfo = this.bindingInfoRepository.GetBindingInfoByRoute(e.Route);
        if (bindingInfo == null)
        {
            this.logger.Error($"binding info for route{e.Route} is not found while listening to the route {e.Route}");
            return;
        }

        MetricProviders.NumberOfReceivedMessagesFromRouter.WithLabels(this.ConnectionId.ToString(), bindingInfo.DataSource, bindingInfo.Stream, e.Key ?? string.Empty)
            .Inc();
        MetricProviders.NumberOfReceivedBytesFromRouter.WithLabels(this.ConnectionId.ToString(), bindingInfo.DataSource, bindingInfo.Stream, e.Key ?? string.Empty)
            .Inc(e.Message.Length);

        this.PacketReceived?.Invoke(
            this,
            new PacketReceivedInfoEventArgs(this.ConnectionId, bindingInfo.DataSource, bindingInfo.Stream, e.Key ?? string.Empty, e.SubmitTime, e.Message));
    }
}
