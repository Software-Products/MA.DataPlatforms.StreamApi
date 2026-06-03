// <copyright file="EssentialReadRequestHandler.cs" company="Motion Applied Ltd.">
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

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.OpenData;
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core.Abstractions;
using MA.Streaming.Proto.Core.Mapper;

namespace MA.Streaming.Proto.Core.Handlers;

public class EssentialReadRequestHandler : IEssentialReadRequestHandler, IBufferConsumer<PacketReceivedInfoEventArgs>
{
    private readonly IEssentialPacketsReaderConnectorService essentialPacketsReaderConnectorService;
    private readonly IMapper<ConnectionInfo, ConnectionDetailsDto> connectionDtoMapper;
    private readonly ServerCallContext context;
    private readonly ILogger logger;
    private readonly ReadEssentialsRequest request;
    private readonly IServerStreamWriter<ReadEssentialsResponse> responseStream;
    private readonly ConnectionDetails connectionDetails;
    private readonly GenericBuffer<PacketReceivedInfoEventArgs> writingBuffer;
    private readonly AutoResetEvent autoResetEvent = new(false);
    private IServerStreamWriter<ReadEssentialsResponse>? currentResponseStream;
    private int stoppedFlag;

    public EssentialReadRequestHandler(
        IEssentialPacketsReaderConnectorService essentialPacketsReaderConnectorService,
        IMapper<ConnectionInfo, ConnectionDetailsDto> connectionDtoMapper,
        ServerCallContext context,
        ILogger logger,
        ReadEssentialsRequest request,
        IServerStreamWriter<ReadEssentialsResponse> responseStream,
        ConnectionDetails connectionDetails)
    {
        this.essentialPacketsReaderConnectorService = essentialPacketsReaderConnectorService;
        this.connectionDtoMapper = connectionDtoMapper;
        this.context = context;
        this.logger = logger;
        this.request = request;
        this.responseStream = responseStream;
        this.connectionDetails = connectionDetails;
        this.writingBuffer = new GenericBuffer<PacketReceivedInfoEventArgs>(this, context.CancellationToken, logger);
    }

    public event EventHandler<DateTime>? HandlingStopped;

    public async Task ConsumeAsync(PacketReceivedInfoEventArgs receivedItem)
    {
        if (this.currentResponseStream == null)
        {
            return;
        }

        try
        {
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
                },
                this.context.CancellationToken);
            MetricProviders.NumberOfEssentialPacketDelivered.WithLabels(receivedItem.ConnectionId.ToString(), receivedItem.DataSource).Inc();
        }
        catch (Exception ex)
        {
            this.StopHandling();
            this.logger.Error($"exception happened in writing essential packet response using stream writer. exception {ex}");
        }
        finally
        {
            await Task.CompletedTask;
        }
    }

    public void Handle()
    {
        this.currentResponseStream = this.responseStream;
        if (this.currentResponseStream == null)
        {
            return;
        }

        this.essentialPacketsReaderConnectorService.PacketReceived += this.EssentialPacketsReaderConnectorService_PacketReceived;

        var connectionInfo = new ConnectionInfo(this.request.Connection.Id, this.connectionDetails);
        _ = Task.Run(
            () =>
            {
                this.essentialPacketsReaderConnectorService.Start(this.connectionDtoMapper.Map(connectionInfo));
            },
            this.context.CancellationToken);

        using var registration = this.context.CancellationToken.Register(this.StopHandling);
        this.autoResetEvent.WaitOne();
    }

    public void StopHandling()
    {
        if (Interlocked.Exchange(ref this.stoppedFlag, 1) == 1)
        {
            return;
        }

        this.essentialPacketsReaderConnectorService.PacketReceived -= this.EssentialPacketsReaderConnectorService_PacketReceived;
        this.essentialPacketsReaderConnectorService.Stop();
        this.autoResetEvent.Set();
        this.HandlingStopped?.Invoke(this, DateTime.Now);
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
}
