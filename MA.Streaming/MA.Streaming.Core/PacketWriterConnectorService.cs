// <copyright file="PacketWriterConnectorService.cs" company="McLaren Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing;
using MA.Streaming.PrometheusMetrics;

namespace MA.Streaming.Core;

public class PacketWriterConnectorService : IPacketWriterConnectorService, IBufferConsumer<WriteDataPacketRequestDto>, IBufferConsumer<WriteInfoPacketRequestDto>
{
    private readonly IDataSourcesRepository dataSourcesRepository;
    private readonly IRouterProvider routerProvider;
    private readonly IRouteNameExtractor routeNameExtractor;
    private readonly ILogger apiLogger;
    private readonly GenericBuffer<WriteDataPacketRequestDto> dataBuffer;
    private readonly GenericBuffer<WriteInfoPacketRequestDto> infoBuffer;

    public PacketWriterConnectorService(
        IDataSourcesRepository dataSourcesRepository,
        IRouterProvider routerProvider,
        IRouteNameExtractor routeNameExtractor,
        ILogger apiLogger)
    {
        this.dataSourcesRepository = dataSourcesRepository;
        this.routerProvider = routerProvider;
        this.routeNameExtractor = routeNameExtractor;
        this.dataBuffer = new GenericBuffer<WriteDataPacketRequestDto>(this, new CancellationTokenSourceProvider());
        this.infoBuffer = new GenericBuffer<WriteInfoPacketRequestDto>(this, new CancellationTokenSourceProvider());
        this.apiLogger = apiLogger;
    }

    public async Task ConsumeAsync(WriteDataPacketRequestDto data)
    {
        try
        {
            this.dataSourcesRepository.Add(data.DataSource);
            var stream = data.Stream;

            if (data.Message.IsEssential)
            {
                this.SendToEssentialsToDefaultEssentialRoute(data);
            }

            var router = this.routerProvider.Provide(data.DataSource, stream);
            router.Route(
                new RoutingDataPacket(
                    data.PacketBytes,
                    this.routeNameExtractor.Extract(data.DataSource, stream),
                    DateTime.UtcNow,
                    data.SessionKey));
            MetricProviders.NumberOfRoutedDataPackets.WithLabels(data.DataSource, stream).Inc();
            MetricProviders.NumberOfRoutedDataPacketsBytes.WithLabels(data.DataSource, stream).Inc(data.PacketBytes.Length);
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
            this.dataSourcesRepository.Add(data.DataSource);
            var router = this.routerProvider.Provide(data.DataSource);
            router.Route(
                new RoutingDataPacket(
                    data.PacketBytes,
                    this.routeNameExtractor.Extract(Constants.SessionInfoDataSourceName),
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

    private void SendToEssentialsToDefaultEssentialRoute(WriteDataPacketRequestDto request)
    {
        var router = this.routerProvider.Provide(request.DataSource, Constants.EssentialStreamName);
        var extract = this.routeNameExtractor.Extract(request.DataSource, Constants.EssentialStreamName);
        router.Route(
            new RoutingDataPacket(
                request.PacketBytes,
                extract,
                DateTime.UtcNow,
                request.SessionKey));
    }
}
