// <copyright file="EssentialReadRequestHandlerFactory.cs" company="Motion Applied Ltd.">
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

using Grpc.Core;

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing.EssentialsRouting;
using MA.Streaming.Proto.Core.Abstractions;
using MA.Streaming.Proto.Core.Handlers;
using MA.Streaming.Proto.Core.Mapper;

namespace MA.Streaming.Proto.Core.Factories;

public class EssentialReadRequestHandlerFactory : IEssentialReadRequestHandlerFactory
{
    private readonly IActiveConnectionManager activeConnectionManager;
    private readonly IEssentialRouteReaderFactory essentialRouteReaderFactory;
    private readonly ILogger logger;
    private readonly IMapper<ConnectionInfo, ConnectionDetailsDto> mapper;

    private readonly SemaphoreSlim mutex = new(1, 1);

    public EssentialReadRequestHandlerFactory(
        IActiveConnectionManager activeConnectionManager,
        IEssentialRouteReaderFactory essentialRouteReaderFactory,
        ILogger logger,
        IMapper<ConnectionInfo, ConnectionDetailsDto> mapper)
    {
        this.activeConnectionManager = activeConnectionManager;
        this.essentialRouteReaderFactory = essentialRouteReaderFactory;
        this.logger = logger;
        this.mapper = mapper;
    }

    public async Task<IEssentialReadRequestHandler?> Create(
        ReadEssentialsRequest request,
        IServerStreamWriter<ReadEssentialsResponse> responseStream,
        ServerCallContext context)
    {
        await this.mutex.WaitAsync();
        try
        {
            this.activeConnectionManager.TryGetConnection(request.Connection.Id, out var connectionDetails);
            if (connectionDetails == null)
            {
                return null;
            }

            var essentialPacketsReaderConnectorService =
                new EssentialPacketsReaderConnectorService(this.essentialRouteReaderFactory, this.logger);
            var handler = new EssentialReadRequestHandler(
                essentialPacketsReaderConnectorService,
                this.mapper,
                context,
                this.logger,
                request,
                responseStream,
                connectionDetails);

            return await Task.FromResult(handler);
        }

        finally
        {
            this.mutex.Release();
        }
    }
}
