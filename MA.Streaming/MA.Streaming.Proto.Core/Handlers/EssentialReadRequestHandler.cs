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

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing;
using MA.Streaming.OpenData;
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core.Abstractions;
using MA.Streaming.Proto.Core.Mapper;

namespace MA.Streaming.Proto.Core.Handlers;

public class EssentialReadRequestHandler : IEssentialReadRequestHandler, IBufferConsumer<PacketReceivedInfoEventArgs>
{
    private readonly IEssentialPacketsReaderConnectorService essentialPacketsReaderConnectorService;
    private readonly IMapper<ConnectionInfo, ConnectionDetailsDto> connectionDtoMapper;
    private readonly ILogger logger;
    private readonly GenericBuffer<PacketReceivedInfoEventArgs> writingBuffer;
    private readonly AutoResetEvent autoResetEvent = new(false);
    private IServerStreamWriter<ReadEssentialsResponse>? currentResponseStream;

    public EssentialReadRequestHandler(
        IEssentialPacketsReaderConnectorService essentialPacketsReaderConnectorService,
        IMapper<ConnectionInfo, ConnectionDetailsDto> connectionDtoMapper,
        ILogger logger)
    {
        this.essentialPacketsReaderConnectorService = essentialPacketsReaderConnectorService;
        this.connectionDtoMapper = connectionDtoMapper;
        this.logger = logger;
        this.writingBuffer = new GenericBuffer<PacketReceivedInfoEventArgs>(this, new CancellationTokenSourceProvider());
    }

    public async Task ConsumeAsync(PacketReceivedInfoEventArgs receivedItem)
    {
        if (this.currentResponseStream == null)
        {
            return;
        }

        try
        {
            if (receivedItem is EndOfReadingPacketReceivedInfoEventArgs)
            {
                this.autoResetEvent.Set();
                return;
            }

            await this.currentResponseStream.WriteAsync(
                new ReadEssentialsResponse
                {
                    Response =
                    {
                        new PacketResponse
                        {
                            Packet = Packet.Parser.ParseFrom(receivedItem.MessageBytes),
                            Stream = receivedItem.Stream,
                            SubmitTime = Timestamp.FromDateTime(receivedItem.SubmitTime)
                        }
                    }
                });
            MetricProviders.NumberOfEssentialPacketDelivered.WithLabels(receivedItem.ConnectionId.ToString(), receivedItem.DataSource).Inc();
        }
        catch (Exception ex)
        {
            this.logger.Error($"exception happened in writing essential packet response using stream writer. exception {ex}");
            this.autoResetEvent.Set();
        }
    }

    public void Handle(
        ReadEssentialsRequest request,
        IServerStreamWriter<ReadEssentialsResponse> responseStream,
        ConnectionDetails connectionDetails)
    {
        this.currentResponseStream = responseStream;
        if (this.currentResponseStream == null)
        {
            return;
        }

        this.essentialPacketsReaderConnectorService.PacketReceived += this.EssentialPacketsReaderConnectorService_PacketReceived;
        var endOfReadingPacketReceivedInfoEventArgs = new EndOfReadingPacketReceivedInfoEventArgs();
        this.essentialPacketsReaderConnectorService.ReadingCompleted += (_, _) =>
        {
            this.essentialPacketsReaderConnectorService.PacketReceived -= this.EssentialPacketsReaderConnectorService_PacketReceived;
            this.WriteDataToStreamAction(endOfReadingPacketReceivedInfoEventArgs);
        };

        this.essentialPacketsReaderConnectorService.Start(this.connectionDtoMapper.Map(new ConnectionInfo(request.Connection.Id, connectionDetails)));

        this.autoResetEvent.WaitOne();
    }

    private void EssentialPacketsReaderConnectorService_PacketReceived(object? sender, PacketReceivedInfoEventArgs e)
    {
        this.WriteDataToStreamAction(e);
        MetricProviders.NumberOfEssentialPacketRead.WithLabels(e.ConnectionId.ToString(), e.DataSource).Inc();
    }

    private void WriteDataToStreamAction(PacketReceivedInfoEventArgs receivedItem)
    {
        this.writingBuffer.AddData(receivedItem);
    }

    private sealed class EndOfReadingPacketReceivedInfoEventArgs : PacketReceivedInfoEventArgs
    {
        public EndOfReadingPacketReceivedInfoEventArgs()
            : base(0, "", "", "", DateTime.MinValue, Array.Empty<byte>())
        {
        }
    }
}
