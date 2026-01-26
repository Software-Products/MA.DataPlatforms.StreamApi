// <copyright file="PacketWriterConnectorService.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing;
using MA.Streaming.PrometheusMetrics;

namespace MA.Streaming.Core;

public class PacketWriterConnectorService : IPacketWriterConnectorService, IBufferConsumer<WriteDataPacketRequestDto>, IBufferConsumer<WriteInfoPacketRequestDto>
{
    private readonly IRouterProvider routerProvider;
    private readonly ILogger apiLogger;
    private readonly GenericBuffer<WriteDataPacketRequestDto> dataBuffer;
    private readonly GenericBuffer<WriteInfoPacketRequestDto> infoBuffer;
    private readonly IDataSourceRoutingInfoTracker dataSourceRoutingInfoTracker;
    private readonly ISessionStreamRoutingPacketTracker sessionStreamRoutingTracker;
    private readonly ISessionPacketWritingLogProvider sessionPacketWritingLogProvider;
    private readonly IStreamStopPacketWriterTracker streamStopPacketWriterTracker;
    private readonly string streamStoppedType;

    public PacketWriterConnectorService(
        IRouterProvider routerProvider,
        IDataSourceRoutingInfoTracker dataSourceRoutingInfoTracker,
        ISessionStreamRoutingPacketTracker sessionStreamRoutingTracker,
        ISessionPacketWritingLogProvider sessionPacketWritingLogProvider,
        ITypeNameProvider typeNameProvider,
        IStreamStopPacketWriterTracker streamStopPacketWriterTracker,
        ILogger apiLogger)
    {
        this.routerProvider = routerProvider;
        this.dataSourceRoutingInfoTracker = dataSourceRoutingInfoTracker;
        this.sessionStreamRoutingTracker = sessionStreamRoutingTracker;
        this.sessionPacketWritingLogProvider = sessionPacketWritingLogProvider;
        this.streamStopPacketWriterTracker = streamStopPacketWriterTracker;
        this.dataBuffer = new GenericBuffer<WriteDataPacketRequestDto>(this, new CancellationTokenSourceProvider());
        this.infoBuffer = new GenericBuffer<WriteInfoPacketRequestDto>(this, new CancellationTokenSourceProvider());
        this.apiLogger = apiLogger;
        this.streamStoppedType = typeNameProvider.StreamStoppedPacketTypeName;
    }

    public async Task ConsumeAsync(WriteDataPacketRequestDto data)
    {
        try
        {
            var result = this.sessionStreamRoutingTracker.Monitor(data);
            if (result.Status is MonitorResultStatus.InvalidPacket)
            {
                this.apiLogger.Info($"not writing the packet: {result.Message}");
                return;
            }

            var sessionWritingLog = this.sessionPacketWritingLogProvider.Provide(data.DataSource, data.SessionKey);
            var kafkaRouteInfo = this.dataSourceRoutingInfoTracker.GetDataRouteInfo(data.DataSource, data.Stream);
            var stream = kafkaRouteInfo.RouteNameInfo.Stream;

            if (data.Message.Type == this.streamStoppedType)
            {
                var processResult = this.streamStopPacketWriterTracker.Track(data, stream);
                if (!processResult.ShouldWriteData)
                {
                    return;
                }

                if (processResult.DataPacketRequestDto != null)
                {
                    data = processResult.DataPacketRequestDto;
                }

                if (processResult.EssentialDataPacketRequestDto != null)
                {
                    this.SendEssentialData(processResult.EssentialDataPacketRequestDto, sessionWritingLog);
                }
            }
            else if (data.Message.IsEssential)
            {
                this.SendEssentialData(data, sessionWritingLog);
            }

            var router = this.routerProvider.ProvideDataStreamRouter(data.DataSource, stream);
            var timeSent = DateTime.UtcNow;
            router.Route(
                new RoutingDataPacket(
                    data.PacketBytes,
                    kafkaRouteInfo.RouteNameInfo.Name,
                    timeSent,
                    data.SessionKey));

            MetricProviders.NumberOfRoutedDataPackets.WithLabels(data.DataSource, stream).Inc();
            MetricProviders.NumberOfRoutedDataPacketsBytes.WithLabels(data.DataSource, stream).Inc(data.PacketBytes.Length);
            sessionWritingLog.Log(stream, data.Message.Type, timeSent);
        }
        catch (Exception ex)
        {
            this.apiLogger.Error(ex.ToString());
        }
        finally
        {
            await Task.CompletedTask;
        }
    }

    public async Task ConsumeAsync(WriteInfoPacketRequestDto data)
    {
        try
        {
            var router = this.routerProvider.ProvideSessionInfoRouter();

            var sessionInfoRouteInfo = this.dataSourceRoutingInfoTracker.GetSessionInfoRouteInfo(data.DataSource);

            router.Route(
                new RoutingDataPacket(
                    data.PacketBytes,
                    sessionInfoRouteInfo.RouteNameInfo.Name,
                    DateTime.UtcNow,
                    data.Message.SessionKey));
            MetricProviders.NumberOfRoutedInfoPackets.WithLabels(data.DataSource).Inc();
            MetricProviders.NumberOfRoutedInfoPacketsBytes.WithLabels(data.DataSource).Inc(data.PacketBytes.Length);
        }
        catch (Exception ex)
        {
            this.apiLogger.Error(ex.ToString());
        }
        finally
        {
            await Task.CompletedTask;
        }
    }

    public void WriteDataPacket(WriteDataPacketRequestDto request)
    {
        this.dataBuffer.AddData(request);
    }

    public void WriteInfoPacket(WriteInfoPacketRequestDto request)
    {
        this.infoBuffer.AddData(request);
    }

    private void SendEssentialData(WriteDataPacketRequestDto request, ISessionPacketWritingLog sessionWritingLog)
    {
        var router = this.routerProvider.ProvideEssentialRouter(request.DataSource);
        var essentialRouteInfo = this.dataSourceRoutingInfoTracker.GetEssentialRouteInfo(request.DataSource);
        var timeSent = DateTime.UtcNow;
        router.Route(
            new RoutingDataPacket(
                request.PacketBytes,
                essentialRouteInfo.RouteNameInfo.Name,
                timeSent,
                request.SessionKey));
        MetricProviders.NumberOfRoutedEssentialDataPackets.WithLabels(request.DataSource, essentialRouteInfo.RouteNameInfo.Stream).Inc();
        MetricProviders.NumberOfRoutedEssentialDataPacketsBytes.WithLabels(request.DataSource, essentialRouteInfo.RouteNameInfo.Stream).Inc(request.PacketBytes.Length);
        sessionWritingLog.Log(essentialRouteInfo.RouteNameInfo.Stream, request.Message.Type, timeSent);
    }
}
