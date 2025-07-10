// <copyright file="ReadDataPacketResponseStreamWriterHandler.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.Core.DataFormatManagement;
using MA.Streaming.OpenData;
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core.Abstractions;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace MA.Streaming.Proto.Core.Handlers;

public class ReadDataPacketResponseStreamWriterHandler : ReadPacketResponseStreamWriterHandlerBase<ReadDataPacketsResponse>, IReadDataPacketResponseStreamWriterHandler
{
    private readonly IServerStreamWriter<ReadDataPacketsResponse> responseStream;
    private readonly IInMemoryRepository<(string dataSource, ulong dataFormatIdentifier, DataFormatTypeDto dataFormatType), DataFormatRecord> dataFormatByUlongIdentifierRepository;
    private readonly IIdentifierFilter parameterIdentifierFilter;
    private readonly IIdentifierFilter eventIdentifierFilter;
    private readonly bool includeMarkers;
    private readonly ConcurrentDictionary<SampleDataFormat, bool> sampleDfFilterCache;
    private readonly ConcurrentDictionary<EventDataFormat, bool> eventDfFilterCache;

    public ReadDataPacketResponseStreamWriterHandler(
        ConnectionDetailsDto connectionDetailsDto,
        IServerStreamWriter<ReadDataPacketsResponse> responseStream,
        ServerCallContext context,
        IPacketReaderConnectorService connectorService,
        bool batchingResponses,
        ILogger logger,
        AutoResetEvent autoResetEvent,
        IIdentifierFilter parameterIdentifierFilter,
        IIdentifierFilter eventIdentifierFilter,
        bool includeMarkers,
        IInMemoryRepository<ValueTuple<string, ulong, DataFormatTypeDto>, DataFormatRecord> dataFormatByUlongIdentifierRepository)
        : base(
            connectionDetailsDto,
            context,
            connectorService,
            batchingResponses,
            logger,
            autoResetEvent)

    {
        this.responseStream = responseStream;
        this.parameterIdentifierFilter = parameterIdentifierFilter;
        this.eventIdentifierFilter = eventIdentifierFilter;
        this.includeMarkers = includeMarkers;
        this.sampleDfFilterCache = new ConcurrentDictionary<SampleDataFormat, bool>();
        this.eventDfFilterCache = new ConcurrentDictionary<EventDataFormat, bool>();

        this.dataFormatByUlongIdentifierRepository = dataFormatByUlongIdentifierRepository;
    }

    public override async Task ConsumeAsync(ReadDataPacketsResponse data)
    {
        await this.responseStream.WriteAsync(data, this.Context.CancellationToken);
    }

    protected internal override async Task WriteDataToStreamActionAsync(IReadOnlyList<PacketReceivedInfoEventArgs> receivedItems)
    {
        try
        {
            // Filter packets 
            var packetResponses = receivedItems.Where(
                i => this.IncludePacket(
                    Packet.Parser.ParseFrom(i.MessageBytes))
            ).Select(
                i => new PacketResponse
                {
                    Packet = Packet.Parser.ParseFrom(i.MessageBytes),
                    Stream = i.Stream,
                    SubmitTime = Timestamp.FromDateTime(i.SubmitTime)
                }).ToList();

            if (packetResponses.Any())
            {
                this.WritingBuffer.AddData(
                    new ReadDataPacketsResponse
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
        }
        catch (Exception ex)
        {
            this.StopHandling();
            this.Logger.Error($"exception happened in writing data packet response using stream writer. exception {ex}");
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
            var packetResponse = new PacketResponse
            {
                Packet = Packet.Parser.ParseFrom(receivedItem.MessageBytes),
                Stream = receivedItem.Stream,
                SubmitTime = Timestamp.FromDateTime(receivedItem.SubmitTime)
            };

            if (!this.IncludePacket(packetResponse.Packet))
            {
                return;
            }

            this.WritingBuffer.AddData(
                new ReadDataPacketsResponse
                {
                    Response =
                    {
                        packetResponse
                    }
                });
        }
        catch (Exception ex)
        {
            this.StopHandling();
            this.Logger.Error($"exception happened in writing data packet response for connection {this.ConnectionId} using stream writer. exception {ex}");
        }
    }

    private bool IncludePacket(Packet packet)
    {
        var content = packet.Content;
        try
        {
            switch (packet.Type)
            {
                case "PeriodicData":
                {
                    var periodicDataPacket = PeriodicDataPacket.Parser.ParseFrom(content);
                    var includePacket = this.FilterDataFormat(this.ConnectionDetailsDto.DataSource, periodicDataPacket.DataFormat);
                    return includePacket;
                }
                case "RowData":
                {
                    var rowDataPacket = RowDataPacket.Parser.ParseFrom(content);
                    var includePacket = this.FilterDataFormat(this.ConnectionDetailsDto.DataSource, rowDataPacket.DataFormat);
                    return includePacket;
                }
                case "SynchroData":
                {
                    var synchroDataPacket = SynchroDataPacket.Parser.ParseFrom(content);
                    var includePacket = this.FilterDataFormat(this.ConnectionDetailsDto.DataSource, synchroDataPacket.DataFormat);
                    return includePacket;
                }
                case "Event":
                {
                    var eventPacket = EventPacket.Parser.ParseFrom(content);
                    var includePacket = this.FilterDataFormat(this.ConnectionDetailsDto.DataSource, eventPacket.DataFormat);
                    return includePacket;
                }
                case "Marker":
                {
                    return this.includeMarkers;
                }
                default:
                {
                        this.Logger.Trace($"Read data packet ignoring packet type{packet.Type}.");
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            this.Logger.Warning($"Unable to handle packet {packet.Type} due to {ex.Message}");
        }

        return false;
    }

    private bool FilterDataFormat(string dataSource, SampleDataFormat dataFormat)
    {
        if (this.sampleDfFilterCache.TryGetValue(dataFormat, out var filter))
        {
            return filter;
        }
        
        var dataFormatIdentifier = dataFormat.DataFormatIdentifier;
        IReadOnlyList<string> parameterIdentifiers;

        if (dataFormat.FormatCase == SampleDataFormat.FormatOneofCase.ParameterIdentifiers)
        {
            parameterIdentifiers = dataFormat.ParameterIdentifiers.ParameterIdentifiers;
        }
        else
        {
            var dataFormatRecord = this.dataFormatByUlongIdentifierRepository.Get(
                (dataSource: dataSource, dataFormatIdentifier: dataFormatIdentifier, dataFormatType: DataFormatTypeDto.Parameter));
            if (dataFormatRecord == null)
            {
                this.Logger.Warning($"No corresponding parameters list for data format identifier: {dataFormatIdentifier}");
                return false;
            }

            parameterIdentifiers = dataFormatRecord.StringIdentifiers;
        }

        // include takes priority in a list
        // i.e. if there is one column that is included, then even if other columns are excluded the packet will still get published. 
        try
        {
            filter = parameterIdentifiers
                .Any(parameterIdentifier => this.parameterIdentifierFilter.ShouldIncludeIdentifier(parameterIdentifier));
            this.sampleDfFilterCache.TryAdd(dataFormat, filter);
            return filter;
        }
        catch (RegexMatchTimeoutException e)
        {
            this.Logger.Warning($"Timeout trying to match the parameter filter {e.Pattern}");
            return false;
        }
    }

    private bool FilterDataFormat(string dataSource, EventDataFormat dataFormat)
    {
        if (this.eventDfFilterCache.TryGetValue(dataFormat, out var filter))
        {
            return filter;
        }
        var dataFormatIdentifier = dataFormat.DataFormatIdentifier;
        string eventIdentifier;

        if (dataFormat.FormatCase == EventDataFormat.FormatOneofCase.EventIdentifier)
        {
            eventIdentifier = dataFormat.EventIdentifier;
        }
        else
        {
            var dataFormatRecord = this.dataFormatByUlongIdentifierRepository.Get(
                (dataSource: dataSource, dataFormatIdentifier: dataFormatIdentifier, dataFormatType: DataFormatTypeDto.Event));
            if (dataFormatRecord == null)
            {
                this.Logger.Warning($"No corresponding event for data format identifier: {dataFormatIdentifier}");
                return false;
            }

            eventIdentifier = dataFormatRecord.StringIdentifiers[0];
        }
        try
        {
            if (this.eventIdentifierFilter.ShouldIncludeIdentifier(eventIdentifier))
            {
                filter = true;
                this.eventDfFilterCache.TryAdd(dataFormat, filter);
                return filter;
            }
        }
        catch (RegexMatchTimeoutException e)
        {
            this.Logger.Warning($"Timeout trying to match the event filter {e.Pattern}");
        }
        filter = false;
        this.eventDfFilterCache.TryAdd(dataFormat, filter);
        return filter;
    }
}
