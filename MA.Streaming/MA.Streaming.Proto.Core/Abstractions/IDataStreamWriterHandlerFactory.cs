// <copyright file="IStreamWriterHandlerFactory.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Proto.Core.Abstractions;

public interface IDataStreamWriterHandlerFactory
{
    Task<IReadDataPacketResponseStreamWriterHandler?> Create(
        DataPacketRequest dataPacketRequest,
        IServerStreamWriter<ReadDataPacketsResponse> serverStreamWriter,
        ServerCallContext context,
        IInMemoryRepository<ValueTuple<string, ulong, DataFormatTypeDto>, DataFormatRecord> dataFormatByUlongIdentifierRepository
        );
}
