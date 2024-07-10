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
using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing;
using MA.Streaming.PrometheusMetrics;

namespace MA.Streaming.Core;

public class PacketWriterConnectorService : IPacketWriterConnectorService
{
    private static readonly object DataRoutingLock = new();
    private readonly IDataSourcesRepository dataSourcesRepository;
    private readonly IRouterProvider routerProvider;
    private readonly IRouteNameExtractor routeNameExtractor;
    private readonly ILogger apiLogger;

    public PacketWriterConnectorService(
        IDataSourcesRepository dataSourcesRepository,
        IRouterProvider routerProvider,
        IRouteNameExtractor routeNameExtractor,
        ILogger apiLogger)
    {
        this.dataSourcesRepository = dataSourcesRepository;
        this.routerProvider = routerProvider;
        this.routeNameExtractor = routeNameExtractor;

        this.apiLogger = apiLogger;
    }

    public void WriteDataPacket(WriteDataPacketRequestDto request)
    {
        lock (DataRoutingLock)
        {
            try
            {
                this.dataSourcesRepository.Add(request.DataSource);
                var stream = request.Stream;

                if (request.Message.IsEssential)
                {
                    this.SendToEssentialsToDefaultEssentialRoute(request);
                }

                var router = this.routerProvider.Provide(request.DataSource, stream);
                router.Route(
                    new RoutingDataPacket(
                        request.PacketBytes,
                        this.routeNameExtractor.Extract(request.DataSource, stream),
                        request.SessionKey));
                MetricProviders.NumberOfRoutedDataPackets.WithLabels(request.DataSource, stream).Inc();
                MetricProviders.NumberOfRoutedDataPacketsBytes.WithLabels(request.DataSource, stream).Inc(request.PacketBytes.Length);
            }
            catch (Exception ex)
            {
                this.apiLogger.Error(ex.ToString());
            }
        }
    }

    private void SendToEssentialsToDefaultEssentialRoute(WriteDataPacketRequestDto request)
    {
        var router = this.routerProvider.Provide(request.DataSource, Constants.EssentialStreamName);
        var extract = this.routeNameExtractor.Extract(request.DataSource, Constants.EssentialStreamName);
        router.Route(
            new RoutingDataPacket(
                request.PacketBytes,
                extract,
                request.SessionKey));
    }

    public void WriteInfoPacket(WriteInfoPacketRequestDto request)
    {
        try
        {
            this.dataSourcesRepository.Add(request.DataSource);
            var router = this.routerProvider.Provide(request.DataSource);
            router.Route(
                new RoutingDataPacket(
                    request.PacketBytes,
                    this.routeNameExtractor.Extract(Constants.SessionInfoDataSourceName),
                    request.Message.SessionKey));
            MetricProviders.NumberOfRoutedInfoPackets.WithLabels(request.DataSource).Inc();
            MetricProviders.NumberOfRoutedInfoPacketsBytes.WithLabels(request.DataSource).Inc(request.PacketBytes.Length);
        }
        catch (Exception ex)
        {
            this.apiLogger.Error(ex.ToString());
        }
    }
}
