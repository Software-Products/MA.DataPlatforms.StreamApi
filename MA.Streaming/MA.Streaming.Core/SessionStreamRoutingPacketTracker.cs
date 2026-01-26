// <copyright file="SessionStreamRoutingPacketTracker.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core;

public class SessionStreamRoutingPacketTracker : ISessionStreamRoutingPacketTracker
{
    private readonly ISessionPacketWritingLogProvider sessionPacketWritingLogProvider;
    private readonly IStreamManager streamManager;
    private readonly ITypeNameProvider typeNameProvider;
    private readonly IDataSourceRoutingInfoTracker dataSourceRoutingInfoTracker;
    private readonly IRouterProvider routerProvider;
    private readonly IStreamStartedPacketRequestFactory startedPacketRequestFactory;

    public SessionStreamRoutingPacketTracker(
        ISessionPacketWritingLogProvider sessionPacketWritingLogProvider,
        IStreamManager streamManager,
        ITypeNameProvider typeNameProvider,
        IDataSourceRoutingInfoTracker dataSourceRoutingInfoTracker,
        IRouterProvider routerProvider,
        IStreamStartedPacketRequestFactory startedPacketRequestFactory)
    {
        this.sessionPacketWritingLogProvider = sessionPacketWritingLogProvider;
        this.streamManager = streamManager;
        this.typeNameProvider = typeNameProvider;
        this.dataSourceRoutingInfoTracker = dataSourceRoutingInfoTracker;
        this.routerProvider = routerProvider;
        this.startedPacketRequestFactory = startedPacketRequestFactory;
    }

    public MonitorResult Monitor(WriteDataPacketRequestDto request)
    {
        if (string.IsNullOrEmpty(request.SessionKey))
        {
            return MonitorResult.ValidPacket;
        }

        var routingInfo = this.dataSourceRoutingInfoTracker.GetDataRouteInfo(request.DataSource, request.Stream);
        if (this.streamManager.CheckIfStreamIsNew(request.DataSource, request.SessionKey, routingInfo.RouteNameInfo.Stream))
        {
            this.SendStartStreamMessage(request, routingInfo);
        }

        return request.Message.Type == this.typeNameProvider.StreamStartedPacketTypeName
            ? MonitorResult.InvalidPacket("the start stream packet already inserted")
            : MonitorResult.ValidPacket;
    }

    private void SendStartStreamMessage(WriteDataPacketRequestDto request, KafkaRouteInfo routingInfo)
    {
        this.TrackEssentialStartingPacket(request.DataSource, request.SessionKey);

        var startStreamPacket = this.startedPacketRequestFactory.CreateStreamStartedPacketWritingRequest(
            request.DataSource,
            request.SessionKey,
            routingInfo.RouteNameInfo.Stream,
            false);
        var router = this.routerProvider.ProvideDataStreamRouter(request.DataSource, routingInfo.RouteNameInfo.Stream);
        var timeSent = DateTime.UtcNow;
        router.Route(
            new RoutingDataPacket(
                startStreamPacket.PacketBytes,
                routingInfo.RouteNameInfo.Name,
                timeSent,
                request.SessionKey));
        this.Log(this.typeNameProvider.StreamStartedPacketTypeName, request.DataSource, request.SessionKey, routingInfo.RouteNameInfo.Stream, timeSent);
    }

    private void TrackEssentialStartingPacket(string dataSource, string sessionKey)
    {
        var essentialRouteInfo = this.dataSourceRoutingInfoTracker.GetEssentialRouteInfo(dataSource);
        if (!this.streamManager.CheckIfStreamIsNew(dataSource, sessionKey, essentialRouteInfo.RouteNameInfo.Stream))
        {
            return;
        }

        this.SendStartStreamMessageToEssential(dataSource, sessionKey);
    }

    private void SendStartStreamMessageToEssential(string dataSource, string sessionKey)
    {
        var essentialRouteInfo = this.dataSourceRoutingInfoTracker.GetEssentialRouteInfo(dataSource);
        var essentialStartStreamPacket = this.startedPacketRequestFactory.CreateStreamStartedPacketWritingRequest(
            dataSource,
            sessionKey,
            essentialRouteInfo.RouteNameInfo.Stream,
            true);
        var router = this.routerProvider.ProvideEssentialRouter(dataSource);
        var timeSent = DateTime.UtcNow;
        router.Route(
            new RoutingDataPacket(
                essentialStartStreamPacket.PacketBytes,
                essentialRouteInfo.RouteNameInfo.Name,
                timeSent,
                sessionKey));

        this.Log(this.typeNameProvider.StreamStartedPacketTypeName, dataSource, sessionKey, essentialRouteInfo.RouteNameInfo.Stream, timeSent);
    }

    private void Log(string packetType, string dataSource, string sessionKey, string streamName, DateTime timeSent)
    {
        var sessionWritingLog = this.sessionPacketWritingLogProvider.Provide(dataSource, sessionKey);
        sessionWritingLog.Log(packetType, streamName, timeSent);
    }
}
