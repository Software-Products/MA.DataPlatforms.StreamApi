// <copyright file="StreamReaderHandlerFactory.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.Proto.Core.Abstractions;
using MA.Streaming.Proto.Core.Handlers;

namespace MA.Streaming.Proto.Core.Factories;

public class StreamReaderHandlerFactory : IStreamReaderHandlerFactory
{
    private readonly IServiceResolver serviceResolver;

    public StreamReaderHandlerFactory(IServiceResolver serviceResolver)
    {
        this.serviceResolver = serviceResolver;
    }

    public IWritePacketRequestStreamReaderHandler Create(
        Guid id,
        IAsyncStreamReader<WriteDataPacketsRequest> asyncStreamReader,
        ServerCallContext context)
    {
        var dtoMapper = this.serviceResolver.Resolve<IMapper<WriteDataPacketsRequest, IReadOnlyList<WriteDataPacketRequestDto>>>();
        var logger = this.serviceResolver.Resolve<ILogger>();
        var packetWriterConnectorService = this.serviceResolver.Resolve<IPacketWriterConnectorService>();
        if (dtoMapper is null ||
            logger is null ||
            packetWriterConnectorService is null)
        {
            throw new InvalidOperationException("Can not create the logger and mapper using service provider");
        }

        return new WritePacketRequestStreamReaderHandler(id, packetWriterConnectorService, asyncStreamReader, context, new AutoResetEvent(false), dtoMapper, logger);
    }
}
