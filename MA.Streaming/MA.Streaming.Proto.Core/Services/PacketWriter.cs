// <copyright file="PacketWriter.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Services;

public sealed class PacketWriter : PacketWriterService.PacketWriterServiceBase
{
    private readonly IPacketWriterConnectorService packetWriterConnectorService;
    private readonly IStreamReaderHandlerFactory streamReaderHandlerFactory;
    private readonly IMapper<WriteDataPacketRequest, WriteDataPacketRequestDto> dataPacketDtoMapper;
    private readonly IMapper<WriteInfoPacketRequest, WriteInfoPacketRequestDto> infoPacketDtoMapper;
    private readonly IMapper<WriteInfoPacketsRequest, WriteInfoPacketRequestDto> infoPacketsDtoMapper;
    private readonly ILogger logger;
    private readonly CancellationTokenSource tokenSource;

    public PacketWriter(
        IPacketWriterConnectorService packetWriterConnectorService,
        IStreamReaderHandlerFactory streamReaderHandlerFactory,
        IMapper<WriteDataPacketRequest, WriteDataPacketRequestDto> dataPacketDtoMapper,
        IMapper<WriteInfoPacketRequest, WriteInfoPacketRequestDto> infoPacketDtoMapper,
        IMapper<WriteInfoPacketsRequest, WriteInfoPacketRequestDto> infoPacketsDtoMapper,
        ICancellationTokenSourceProvider cancellationTokenSourceProvider,
        ILogger logger)
    {
        this.packetWriterConnectorService = packetWriterConnectorService;
        this.streamReaderHandlerFactory = streamReaderHandlerFactory;
        this.dataPacketDtoMapper = dataPacketDtoMapper;
        this.infoPacketDtoMapper = infoPacketDtoMapper;
        this.infoPacketsDtoMapper = infoPacketsDtoMapper;
        this.logger = logger;
        this.tokenSource = cancellationTokenSourceProvider.Provide();
    }

    public override Task<WriteDataPacketResponse> WriteDataPacket(WriteDataPacketRequest request, ServerCallContext context)
    {
        var writeDataPacketRequestDto = this.dataPacketDtoMapper.Map(request);
        this.packetWriterConnectorService.WriteDataPacket(writeDataPacketRequestDto);
        MetricProviders.NumberOfDataPacketsPublished.WithLabels(request.Detail.DataSource, request.Detail.Stream).Inc();
        return Task.FromResult(new WriteDataPacketResponse());
    }

    public override async Task<WriteDataPacketsResponse> WriteDataPackets(IAsyncStreamReader<WriteDataPacketsRequest> requestStream, ServerCallContext context)
    {
        var requestId = Guid.NewGuid();
        this.streamReaderHandlerFactory.Create(requestId, requestStream, context).StartHandling();
        return await Task.FromResult(new WriteDataPacketsResponse());
    }

    public override async Task<WriteInfoPacketResponse> WriteInfoPacket(WriteInfoPacketRequest request, ServerCallContext context)
    {
        var writeInfoPacketRequestDto = this.infoPacketDtoMapper.Map(request);
        this.packetWriterConnectorService.WriteInfoPacket(writeInfoPacketRequestDto);
        MetricProviders.NumberOfInfoPacketsPublished.WithLabels(writeInfoPacketRequestDto.DataSource).Inc();
        return await Task.FromResult(new WriteInfoPacketResponse());
    }

    public override async Task<WriteInfoPacketsResponse> WriteInfoPackets(IAsyncStreamReader<WriteInfoPacketsRequest> requestStream, ServerCallContext context)
    {
        var canContinue = true;
        while (canContinue && !this.tokenSource.IsCancellationRequested)
        {
            try
            {
                await foreach (var infoPacketsRequest in requestStream.ReadAllAsync())
                {
                    var writeInfoPacketRequestDto = this.infoPacketsDtoMapper.Map(infoPacketsRequest);
                    MetricProviders.NumberOfInfoPacketsPublished.WithLabels(
                        writeInfoPacketRequestDto.DataSource).Inc();
                    this.packetWriterConnectorService.WriteInfoPacket(writeInfoPacketRequestDto);
                }
            }
            catch (Exception ex)
            {
                this.logger.Error(ex.ToString());
                canContinue = false;
            }

            Task.Delay(20).Wait();
        }

        return await Task.FromResult(new WriteInfoPacketsResponse());
    }
}
