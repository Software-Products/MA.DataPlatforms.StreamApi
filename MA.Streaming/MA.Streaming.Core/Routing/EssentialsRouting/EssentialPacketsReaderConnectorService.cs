// <copyright file="EssentialPacketsReaderConnectorService.cs" company="McLaren Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouteReaderComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;

namespace MA.Streaming.Core.Routing.EssentialsRouting;

public class EssentialPacketsReaderConnectorService : IEssentialPacketsReaderConnectorService
{
    private readonly IEssentialRouteReaderFactory essentialRouteReaderFactory;
    private readonly IRouteBindingInfoRepository bindingInfoRepository;
    private readonly ILogger logger;

    private long connectionId;
    private string? sessionKey;
    private IRouteReader reader;

    public EssentialPacketsReaderConnectorService(
        IEssentialRouteReaderFactory essentialRouteReaderFactory,
        IRouteBindingInfoRepository bindingInfoRepository,
        ILogger logger)
    {
        this.essentialRouteReaderFactory = essentialRouteReaderFactory;
        this.bindingInfoRepository = bindingInfoRepository;
        this.logger = logger;
    }

    public event EventHandler<PacketReceivedInfoEventArgs>? PacketReceived;

    public event EventHandler<EssentialReadingCompletedEventArgs>? ReadingCompleted;

    public void Start(ConnectionDetailsDto connectionDetailsDto)
    {
        this.connectionId = connectionDetailsDto.Id;
        this.sessionKey = connectionDetailsDto.SessionKey;
        this.reader = this.essentialRouteReaderFactory.Create(connectionDetailsDto);
        this.reader.PacketReceived += this.Reader_PacketReceived;
        this.reader.ReadingCompleted += this.Reader_ReadingCompleted;
        this.reader.StartReading();
    }

    private void Reader_ReadingCompleted(object? sender, ReadingCompletedEventArgs e)
    {
        this.ReadingCompleted?.Invoke(this, new EssentialReadingCompletedEventArgs(e.FinishedTime, e.NumberOfItemsRead));
        this.reader.PacketReceived -= this.Reader_PacketReceived;
        this.reader.ReadingCompleted -= this.Reader_ReadingCompleted;
    }

    private void Reader_PacketReceived(object? sender, RoutingDataPacket e)
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

        if (bindingInfo.Stream != Constants.EssentialStreamName)
        {
            return;
        }

        this.PacketReceived?.Invoke(
            this,
            new PacketReceivedInfoEventArgs(this.connectionId, bindingInfo.DataSource, string.Empty, e.Key ?? string.Empty, e.SubmitTime, e.Message));
    }
}
