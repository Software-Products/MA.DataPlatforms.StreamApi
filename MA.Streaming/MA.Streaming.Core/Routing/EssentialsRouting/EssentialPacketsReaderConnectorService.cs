// <copyright file="EssentialPacketsReaderConnectorService.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing.EssentialsRouting;

public class EssentialPacketsReaderConnectorService : IEssentialPacketsReaderConnectorService
{
    private readonly IEssentialRouteReaderFactory essentialRouteReaderFactory;

    private long connectionId;
    private string? sessionKey;
    private IRouteSubscriber? routeSubscriber;
    private readonly ILogger logger;
    private string? dataSource;

    public EssentialPacketsReaderConnectorService(IEssentialRouteReaderFactory essentialRouteReaderFactory, ILogger logger)
    {
        this.essentialRouteReaderFactory = essentialRouteReaderFactory;
        this.logger = logger;
    }

    public event EventHandler<PacketReceivedInfoEventArgs>? PacketReceived;

    public void Start(ConnectionDetailsDto connectionDetailsDto)
    {
        this.connectionId = connectionDetailsDto.Id;
        this.sessionKey = connectionDetailsDto.SessionKey;
        this.dataSource = connectionDetailsDto.DataSource;
        this.routeSubscriber = this.essentialRouteReaderFactory.Create(connectionDetailsDto);
        this.routeSubscriber.PacketReceived += this.Reader_PacketReceived;
        this.routeSubscriber.Subscribe();
    }

    public void Stop()
    {
        try
        {
            this.logger.Info($"stopping essential reader {this.sessionKey}-{this.connectionId}");
            if (this.routeSubscriber == null)
            {
                return;
            }

            this.routeSubscriber.PacketReceived -= this.Reader_PacketReceived;
            this.routeSubscriber.Unsubscribe();
            this.logger.Info($"essential reader {this.sessionKey}-{this.connectionId} stopped");
        }
        catch (Exception ex)
        {
            this.logger.Error($"Exception happened on stopping the reader connector service subscriber {ex}");
        }
    }

    private void Reader_PacketReceived(object? sender, RoutingDataPacket e)
    {
        if (string.IsNullOrEmpty(this.sessionKey) ||
            e.Key != this.sessionKey ||
            string.IsNullOrEmpty(this.dataSource))
        {
            return;
        }

        var packetReceivedInfoEventArgs = new PacketReceivedInfoEventArgs(
            this.connectionId,
            this.dataSource,
            string.Empty,
            e.Key ?? string.Empty,
            e.SubmitTime,
            e.Message);

        this.PacketReceived?.Invoke(
            this,
            packetReceivedInfoEventArgs);
    }
}
