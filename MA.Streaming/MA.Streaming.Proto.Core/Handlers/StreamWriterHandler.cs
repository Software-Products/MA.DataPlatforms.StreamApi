// <copyright file="StreamWriterHandler.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.Core;
using MA.Streaming.OpenData;
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Handlers;

public class StreamWriterHandler : IStreamWriterHandler
{
    private readonly ConnectionDetailsDto connectionDetailsDto;
    private readonly IServerStreamWriter<ReadPacketsResponse> responseStream;
    private readonly ServerCallContext context;
    private readonly IPacketReaderConnectorService connectorService;
    private readonly bool batchingResponses;
    private readonly ILogger logger;
    private readonly AutoResetEvent autoResetEvent;
    private readonly TimeAndSizeWindowBatchProcessor<PacketReceivedInfoEventArgs>? timeWindowBatchProcessor;
    private static readonly object WritingLocK = new();
    private bool started;

    public StreamWriterHandler(
        ConnectionDetailsDto connectionDetailsDto,
        IServerStreamWriter<ReadPacketsResponse> responseStream,
        ServerCallContext context,
        IPacketReaderConnectorService connectorService,
        bool batchingResponses,
        ILogger logger,
        AutoResetEvent autoResetEvent)
    {
        this.connectionDetailsDto = connectionDetailsDto;
        this.responseStream = responseStream;
        this.context = context;
        this.connectorService = connectorService;
        this.batchingResponses = batchingResponses;
        this.logger = logger;
        this.autoResetEvent = autoResetEvent;
        this.ConnectionId = connectionDetailsDto.Id;
        this.connectorService.PacketReceived += this.ConnectorService_PacketReceived;
        if (batchingResponses)
        {
            this.timeWindowBatchProcessor =
                new TimeAndSizeWindowBatchProcessor<PacketReceivedInfoEventArgs>(this.WriteDataToStreamActionAsync, new CancellationTokenSource());
        }
    }

    private void ConnectorService_PacketReceived(object? sender, PacketReceivedInfoEventArgs e)
    {
        MetricProviders.NumberOfDataPacketRead.WithLabels(this.ConnectionId.ToString(), e.DataSource, e.Stream)
            .Inc();
        if (this.batchingResponses &&
            this.timeWindowBatchProcessor != null)
        {
            this.timeWindowBatchProcessor.Add(e);
        }
        else
        {
            this.WriteDataToStreamAction(e);
        }
    }

    public long ConnectionId { get; }

    public void StartHandling()
    {
        if (this.started)
        {
            return;
        }

        _ = Task.Run(
            () =>
            {
                this.connectorService.Start();
            },
            this.context.CancellationToken);
        this.started = true;
        this.autoResetEvent.WaitOne();
    }

    public void StopHandling()
    {
        this.connectorService.Stop();
        this.autoResetEvent.Set();
        this.HandlingStopped?.Invoke(this, DateTime.Now);
    }

    public event EventHandler<DateTime>? HandlingStopped;

    private async Task WriteDataToStreamActionAsync(IReadOnlyList<PacketReceivedInfoEventArgs> receivedItems)
    {
        try
        {
            var packetResponses = receivedItems.Select(
                i => new PacketResponse
                {
                    Packet = Packet.Parser.ParseFrom(i.MessageBytes),
                    Stream = i.Stream
                }).ToList();
            await this.responseStream.WriteAsync(
                new ReadPacketsResponse
                {
                    Response =
                    {
                        packetResponses
                    }
                },
                this.context.CancellationToken);
            lock (WritingLocK)
            {
                var streamItems = receivedItems.GroupBy(i => i.Stream);
                foreach (var streamItem in streamItems)
                {
                    var increment = streamItem.Count();
                    MetricProviders.NumberOfDataPacketDelivered.WithLabels(this.ConnectionId.ToString(), this.connectionDetailsDto.DataSource, streamItem.Key)
                        .Inc(increment);
                }
            }
        }
        catch (Exception ex)
        {
            this.StopHandling();
            this.logger.Error($"exception happened in writing packet response using stream writer. exception {ex}");
        }
    }

    private void WriteDataToStreamAction(PacketReceivedInfoEventArgs receivedItem)
    {
        lock (WritingLocK)
        {
            try
            {
                this.responseStream.WriteAsync(
                    new ReadPacketsResponse
                    {
                        Response =
                        {
                            new PacketResponse
                            {
                                Packet = Packet.Parser.ParseFrom(receivedItem.MessageBytes),
                                Stream = receivedItem.Stream
                            }
                        }
                    },
                    this.context.CancellationToken).Wait(this.context.CancellationToken);
                MetricProviders.NumberOfDataPacketDelivered.WithLabels(this.ConnectionId.ToString(), this.connectionDetailsDto.DataSource, receivedItem.Stream)
                    .Inc();
            }
            catch (Exception ex)
            {
                this.StopHandling();
                this.logger.Error($"exception happened in writing packet response for connection {this.ConnectionId} using stream writer. exception {ex}");
            }
        }
    }
}
