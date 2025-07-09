// <copyright file="PacketReader.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core.DataFormatManagement;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Services;

public sealed class PacketReader : PacketReaderService.PacketReaderServiceBase
{
    private readonly IActiveConnectionManager activeConnectionManager;
    private readonly IStreamWriterHandlerFactory streamWriterHandlerFactory;
    private readonly IEssentialReadRequestHandler essentialReadRequestHandler;
    private readonly IDataStreamWriterHandlerFactory dataStreamWriterHandlerFactory;
    private readonly IInMemoryRepository<(string, ulong, DataFormatTypeDto), DataFormatRecord> dataFormatByUlongIdentifierRepository;

    public PacketReader(
        IActiveConnectionManager activeConnectionManager,
        IStreamWriterHandlerFactory streamWriterHandlerFactory,
        IEssentialReadRequestHandler essentialReadRequestHandler,
        IDataStreamWriterHandlerFactory dataStreamWriterHandlerFactory,
        IInMemoryRepository<ValueTuple<string, ulong, DataFormatTypeDto>, DataFormatRecord> dataFormatByUlongIdentifierRepository
        )
    {
        this.activeConnectionManager = activeConnectionManager;
        this.streamWriterHandlerFactory = streamWriterHandlerFactory;
        this.essentialReadRequestHandler = essentialReadRequestHandler;
        this.dataStreamWriterHandlerFactory = dataStreamWriterHandlerFactory;
        this.dataFormatByUlongIdentifierRepository = dataFormatByUlongIdentifierRepository;
    }

    public override async Task ReadPackets(ReadPacketsRequest request, IServerStreamWriter<ReadPacketsResponse> responseStream, ServerCallContext context)
    {
        var handler = await this.streamWriterHandlerFactory.Create(request.Connection.Id, responseStream, context);
        handler?.StartHandling();
    }

    public override Task ReadEssentials(ReadEssentialsRequest request, IServerStreamWriter<ReadEssentialsResponse> responseStream, ServerCallContext context)
    {
        this.activeConnectionManager.TryGetConnection(request.Connection.Id, out var connectionDetails);
        if (connectionDetails == null)
        {
            return Task.CompletedTask;
        }

        this.essentialReadRequestHandler.Handle(request, responseStream, connectionDetails);

        return Task.CompletedTask;
    }

    public override async Task ReadDataPackets(ReadDataPacketsRequest request, IServerStreamWriter<ReadDataPacketsResponse> responseStream, ServerCallContext context)
    {
        var handler = await this.dataStreamWriterHandlerFactory.Create(request.Request, responseStream, context, this.dataFormatByUlongIdentifierRepository);

        handler?.StartHandling();
    }
}
