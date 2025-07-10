// <copyright file="ReadPacketResponseStreamWriterHandler.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.OpenData;
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Handlers;

public class ReadPacketResponseStreamWriterHandler : ReadPacketResponseStreamWriterHandlerBase<ReadPacketsResponse>, IReadPacketResponseStreamWriterHandler
{
    private readonly IServerStreamWriter<ReadPacketsResponse> responseStream;

    public ReadPacketResponseStreamWriterHandler(
        ConnectionDetailsDto connectionDetailsDto,
        IServerStreamWriter<ReadPacketsResponse> responseStream,
        ServerCallContext context,
        IPacketReaderConnectorService connectorService,
        bool batchingResponses,
        ILogger logger,
        AutoResetEvent autoResetEvent)
        : base(
            connectionDetailsDto,
            context,
            connectorService,
            batchingResponses,
            logger,
            autoResetEvent)
    {
        this.responseStream = responseStream;
    }

    public override async Task ConsumeAsync(ReadPacketsResponse data)
    {
        await this.responseStream.WriteAsync(data, this.Context.CancellationToken);
    }

    protected internal override async Task WriteDataToStreamActionAsync(IReadOnlyList<PacketReceivedInfoEventArgs> receivedItems)
    {
        try
        {
            var packetResponses = receivedItems.Select(
                i => new PacketResponse
                {
                    Packet = Packet.Parser.ParseFrom(i.MessageBytes),
                    Stream = i.Stream,
                    SubmitTime = Timestamp.FromDateTime(i.SubmitTime)
                }).ToList();

            this.WritingBuffer.AddData(
                new ReadPacketsResponse
                {
                    Response =
                    {
                        packetResponses
                    }
                });
            var streamItems = receivedItems.GroupBy(i => i.Stream);
            foreach (var streamItem in streamItems)
            {
                var increment = streamItem.Count();
                MetricProviders.NumberOfDataPacketDelivered.WithLabels(this.ConnectionId.ToString(), this.ConnectionDetailsDto.DataSource, streamItem.Key)
                    .Inc(increment);
            }
        }
        catch (Exception ex)
        {
            this.StopHandling();
            this.Logger.Error($"exception happened in writing packet response using stream writer. exception {ex}");
        }
        finally
        {
            await Task.CompletedTask;
        }
    }

    protected internal override void WriteDataToStreamAction(PacketReceivedInfoEventArgs receivedItem)
    {
        try
        {
            this.WritingBuffer.AddData(
                new ReadPacketsResponse
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
        }
        catch (Exception ex)
        {
            this.StopHandling();
            this.Logger.Error($"exception happened in writing packet response for connection {this.ConnectionId} using stream writer. exception {ex}");
        }
    }
}
