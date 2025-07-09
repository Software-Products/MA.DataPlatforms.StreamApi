// <copyright file="WritePacketRequestStreamReaderHandler.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Proto.Core.Handlers;

public class WritePacketRequestStreamReaderHandler : IWritePacketRequestStreamReaderHandler
{
    private readonly IPacketWriterConnectorService packetWriterConnectorService;
    private readonly IAsyncStreamReader<WriteDataPacketsRequest> asyncStreamReader;
    private readonly ServerCallContext context;
    private readonly AutoResetEvent autoResetEvent;
    private readonly ILogger logger;
    private readonly IMapper<WriteDataPacketsRequest, IReadOnlyList<WriteDataPacketRequestDto>> dataPacketsDtoMapper;

    public WritePacketRequestStreamReaderHandler(
        Guid id,
        IPacketWriterConnectorService packetWriterConnectorService,
        IAsyncStreamReader<WriteDataPacketsRequest> asyncStreamReader,
        ServerCallContext context,
        AutoResetEvent autoResetEvent,
        IMapper<WriteDataPacketsRequest, IReadOnlyList<WriteDataPacketRequestDto>> dataPacketsDtoMapper,
        ILogger logger)
    {
        this.packetWriterConnectorService = packetWriterConnectorService;
        this.asyncStreamReader = asyncStreamReader;
        this.context = context;
        this.autoResetEvent = autoResetEvent;
        this.logger = logger;
        this.dataPacketsDtoMapper = dataPacketsDtoMapper;
        this.Id = id;
    }

    public Guid Id { get; }

    public void StartHandling()
    {
        _ = Task.Run(
            async () =>
            {
                while (!this.context.CancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        await foreach (var writeDataPacketRequest in this.asyncStreamReader.ReadAllAsync())
                        {
                            foreach (var writeDataPacketRequestDto in this.dataPacketsDtoMapper.Map(writeDataPacketRequest))
                            {
                                MetricProviders.NumberOfDataPacketsPublished.WithLabels(
                                    writeDataPacketRequestDto.DataSource,
                                    writeDataPacketRequestDto.Stream).Inc();
                                this.packetWriterConnectorService.WriteDataPacket(writeDataPacketRequestDto);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        this.logger.Error(ex.ToString());
                        this.StopHandling();
                        break;
                    }

                    Task.Delay(5).ConfigureAwait(false).GetAwaiter().GetResult();
                }
            });

        this.autoResetEvent.WaitOne();
    }

    public void StopHandling()
    {
        this.autoResetEvent.Set();
        this.HandlingStopped?.Invoke(this, DateTime.Now);
    }

    public event EventHandler<DateTime>? HandlingStopped;
}
