// <copyright file="EssentialReadRequestHandler.cs" company="McLaren Applied Ltd.">
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

using Grpc.Core;

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.OpenData;
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core.Abstractions;
using MA.Streaming.Proto.Core.Mapper;

namespace MA.Streaming.Proto.Core.Handlers;

public class EssentialReadRequestHandler : IEssentialReadRequestHandler
{
    private readonly IEssentialPacketsReaderConnectorService essentialPacketsReaderConnectorService;
    private readonly IMapper<ConnectionInfo, ConnectionDetailsDto> connectionDtoMapper;
    private readonly ILogger logger;

    private readonly object writingLock = new();
    private IServerStreamWriter<ReadEssentialsResponse>? currentResponseStream;

    public EssentialReadRequestHandler(
        IEssentialPacketsReaderConnectorService essentialPacketsReaderConnectorService,
        IMapper<ConnectionInfo, ConnectionDetailsDto> connectionDtoMapper,
        ILogger logger)
    {
        this.essentialPacketsReaderConnectorService = essentialPacketsReaderConnectorService;
        this.connectionDtoMapper = connectionDtoMapper;
        this.logger = logger;
    }

    public void Handle(
        ReadEssentialsRequest request,
        IServerStreamWriter<ReadEssentialsResponse> responseStream,
        ConnectionDetails connectionDetails)
    {
        var autoResetEvent = new AutoResetEvent(false);
        this.currentResponseStream = responseStream;
        this.essentialPacketsReaderConnectorService.PacketReceived += (_, e) =>
        {
            this.WriteDataToStreamAction(e);
            MetricProviders.NumberOfEssentialPacketRead.WithLabels(e.ConnectionId.ToString(), e.DataSource).Inc();
        };
        this.essentialPacketsReaderConnectorService.ReadingCompleted += (_, _) =>
        {
            autoResetEvent.Set();
        };

        this.essentialPacketsReaderConnectorService.Start(this.connectionDtoMapper.Map(new ConnectionInfo(request.Connection.Id, connectionDetails)));

        autoResetEvent.WaitOne();
    }

    private void WriteDataToStreamAction(PacketReceivedInfoEventArgs receivedItem)
    {
        lock (this.writingLock)
        {
            if (this.currentResponseStream == null)
            {
                return;
            }

            try
            {
                this.currentResponseStream.WriteAsync(
                    new ReadEssentialsResponse
                    {
                        Response =
                        {
                            new PacketResponse
                            {
                                Packet = Packet.Parser.ParseFrom(receivedItem.MessageBytes),
                                Stream = receivedItem.Stream
                            }
                        }
                    }).Wait();
                MetricProviders.NumberOfEssentialPacketDelivered.WithLabels(receivedItem.ConnectionId.ToString(), receivedItem.DataSource).Inc();
            }
            catch (Exception ex)
            {
                this.logger.Error($"exception happened in writing essential packet response using stream writer. exception {ex}");
            }
        }
    }
}
