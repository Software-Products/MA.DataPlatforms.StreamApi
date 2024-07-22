// <copyright file="StreamWriterHandlerFactory.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing;
using MA.Streaming.Proto.Core.Abstractions;
using MA.Streaming.Proto.Core.Handlers;
using MA.Streaming.Proto.Core.Mapper;

namespace MA.Streaming.Proto.Core.Factories;

public class StreamWriterHandlerFactory : IStreamWriterHandlerFactory
{
    private readonly IInMemoryRepository<long, IReadPacketResponseStreamWriterHandler> handlerRepository;
    private readonly IRouteSubscriberFactory routeSubscriberFactory;
    private readonly IActiveConnectionManager activeConnectionManager;
    private readonly IMapper<ConnectionInfo, ConnectionDetailsDto> connectionDtoMapper;
    private readonly ILogger logger;
    private readonly IStreamingApiConfiguration config;
    private readonly object lockObject = new();

    public StreamWriterHandlerFactory(
        IInMemoryRepository<long, IReadPacketResponseStreamWriterHandler> handlerRepository,
        IRouteSubscriberFactory routeSubscriberFactory,
        IActiveConnectionManager activeConnectionManager,
        IMapper<ConnectionInfo, ConnectionDetailsDto> connectionDtoMapper,
        ILogger logger,
        IStreamingApiConfigurationProvider apiConfigurationProvider)
    {
        this.handlerRepository = handlerRepository;
        this.routeSubscriberFactory = routeSubscriberFactory;
        this.activeConnectionManager = activeConnectionManager;
        this.connectionDtoMapper = connectionDtoMapper;
        this.logger = logger;
        this.config = apiConfigurationProvider.Provide();
    }

    public IReadPacketResponseStreamWriterHandler? Create(long connectionId, IServerStreamWriter<ReadPacketsResponse> serverStreamWriter, ServerCallContext context)
    {
        lock (this.lockObject)
        {
            var handler = this.handlerRepository.Get(connectionId);
            if (handler != null)
            {
                return handler;
            }

            this.activeConnectionManager.TryGetConnection(connectionId, out var connectionDetails);
            if (connectionDetails is null)
            {
                return null;
            }

            var connectionDetailsDto = this.connectionDtoMapper.Map(new ConnectionInfo(connectionId, connectionDetails));
            var routeBindingInfoRepository = new RouteBindingInfoRepository();
            var connectorService = new PacketReaderConnectorService(
                connectionDetailsDto,
                this.routeSubscriberFactory.Create(connectionDetailsDto, routeBindingInfoRepository),
                this.logger,
                routeBindingInfoRepository);
            handler = new ReadPacketResponseStreamWriterHandler(
                connectionDetailsDto,
                serverStreamWriter,
                context,
                connectorService,
                this.config.BatchingResponses,
                this.logger,
                new AutoResetEvent(false));
            return handler;
        }
    }
}
