// <copyright file="ServiceConfigurator.cs" company="McLaren Applied Ltd.">
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

using MA.Common;
using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.DataFormatManagement;
using MA.Streaming.Core.Routing;
using MA.Streaming.Core.Routing.EssentialsRouting;
using MA.Streaming.Core.Routing.PartitionsRouting;
using MA.Streaming.Core.Routing.TopicsRouting;
using MA.Streaming.Core.SessionManagement;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Core;
using MA.Streaming.Proto.Core.Abstractions;
using MA.Streaming.Proto.Core.Factories;
using MA.Streaming.Proto.Core.Handlers;
using MA.Streaming.Proto.Core.Mapper;
using MA.Streaming.Proto.Core.Model;
using MA.Streaming.Proto.Core.Providers;
using MA.Streaming.Proto.Core.Services;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using ILogger = MA.Common.Abstractions.ILogger;

namespace MA.Streaming.Proto.ServerComponent;

public class ServiceConfigurator
{
    private readonly IStreamingApiConfiguration streamingApiConfiguration;
    private readonly ICancellationTokenSourceProvider cancellationTokenSourceProvider;
    private readonly ILoggingDirectoryProvider loggingDirectoryProvider;

    public ServiceConfigurator(
        IStreamingApiConfiguration streamingApiConfiguration,
        ICancellationTokenSourceProvider cancellationTokenSourceProvider,
        ILoggingDirectoryProvider loggingDirectoryProvider)
    {
        this.streamingApiConfiguration = streamingApiConfiguration;
        this.cancellationTokenSourceProvider = cancellationTokenSourceProvider;
        this.loggingDirectoryProvider = loggingDirectoryProvider;
    }

    public void Configure(IServiceCollection serviceCollection, bool registerServices)
    {
        ConfigureLogging(
            serviceCollection,
            logging =>
            {
                logging.ClearProviders();
                logging.SetMinimumLevel(LogLevel.Warning);
                logging.AddConsole();
            });

        serviceCollection.AddSingleton<IStreamingApiConfigurationProvider>(new StreamingApiConfigurationProvider(this.streamingApiConfiguration));
        serviceCollection.AddSingleton<IRouteManager, KafkaRouteManager>();
        serviceCollection.AddSingleton<IActiveConnectionManager, ActiveConnectionManager>();
        serviceCollection.AddSingleton<IRouteBindingInfoRepository, RouteBindingInfoRepository>();
        serviceCollection.AddSingleton<IStreamWriterHandlerFactory, StreamWriterHandlerFactory>();
        serviceCollection.AddSingleton(this.cancellationTokenSourceProvider);
        serviceCollection.AddSingleton<IInMemoryRepository<string, SessionDetailRecord>, ThreadSafeInMemoryRepository<string, SessionDetailRecord>>();
        serviceCollection.AddSingleton<IInMemoryRepository<string, List<DataFormatRecord>>, ThreadSafeInMemoryRepository<string, List<DataFormatRecord>>>();
        serviceCollection.AddSingleton<IDataFormatInfoService, DataFormatInfoService>();
        serviceCollection.AddSingleton<ISessionInfoService, SessionInfoService>();
        serviceCollection.AddSingleton<IStreamReaderHandlerFactory, StreamReaderHandlerFactory>();
        serviceCollection.AddSingleton<IDataSourcesRepository, DataSourcesRepository>();
        serviceCollection.AddSingleton<ISessionNotificationManagerService, SessionNotificationManagerService>();

        serviceCollection
            .AddSingleton<IInMemoryRepository<long, IReadPacketResponseStreamWriterHandler>,
                ThreadSafeInMemoryRepository<long, IReadPacketResponseStreamWriterHandler>>();

        serviceCollection
            .AddSingleton<IInMemoryRepository<long, IReadDataPacketResponseStreamWriterHandler>,
                ThreadSafeInMemoryRepository<long, IReadDataPacketResponseStreamWriterHandler>>();

        serviceCollection
            .AddSingleton<IInMemoryRepository<ValueTuple<string, string, DataFormatTypeDto>, DataFormatRecord>,
                ThreadSafeInMemoryRepository<ValueTuple<string, string, DataFormatTypeDto>, DataFormatRecord>>();
        serviceCollection
            .AddSingleton<IInMemoryRepository<(string, ulong, DataFormatTypeDto), DataFormatRecord>,
                ThreadSafeInMemoryRepository<(string, ulong, DataFormatTypeDto), DataFormatRecord>>();

        serviceCollection
            .AddSingleton<INotificationStreamWriterService<GetSessionStartNotificationResponse>,
                NotificationStreamWriterService<GetSessionStartNotificationResponse>>();
        serviceCollection
            .AddSingleton<INotificationStreamWriterService<GetSessionStopNotificationResponse>,
                NotificationStreamWriterService<GetSessionStopNotificationResponse>>();

        serviceCollection.AddSingleton<IPacketWriterConnectorService, PacketWriterConnectorService>();
        serviceCollection.AddSingleton<IStreamsProvider, StreamsProvider>();

        serviceCollection.AddTransient(_ => this.loggingDirectoryProvider);
        serviceCollection.AddTransient<ISessionCreationRequestHandler, SessionCreationRequestHandler>();
        serviceCollection.AddTransient<ISessionEndingRequestHandler, SessionEndingRequestHandler>();
        serviceCollection.AddTransient<IAddAssociateSessionRequestHandler, AddAssociateSessionRequestHandler>();
        serviceCollection.AddTransient<ISessionIdentifierUpdateRequestHandler, SessionIdentifierUpdateRequestHandler>();
        serviceCollection.AddTransient<ISessionDetailsUpdateRequestHandler, SessionDetailsUpdateRequestHandler>();
        serviceCollection.AddTransient<IGetSessionInfoRequestHandler, GetSessionInfoRequestHandler>();
        serviceCollection.AddTransient<IPacketWriterHelper, PacketWriterHelper>();
        serviceCollection.AddTransient<IMapper<Packet, PacketDto>, PacketDtoMapper>();
        serviceCollection.AddTransient<IMapper<WriteDataPacketRequest, WriteDataPacketRequestDto>, WriteDataPacketRequestDtoMapper>();
        serviceCollection.AddTransient<IMapper<WriteDataPacketsRequest, IReadOnlyList<WriteDataPacketRequestDto>>, WriteDataPacketsRequestDtoMapper>();
        serviceCollection.AddTransient<IMapper<WriteInfoPacketRequest, WriteInfoPacketRequestDto>, WriteInfoPacketRequestDtoMapper>();
        serviceCollection.AddTransient<IMapper<WriteInfoPacketsRequest, WriteInfoPacketRequestDto>, WriteInfoPacketsRequestDtoMapper>();
        serviceCollection.AddTransient<IMapper<ConnectionInfo, ConnectionDetailsDto>, ConnectionDetailDtoMapper>();
        serviceCollection.AddTransient<ILogger, MicrosoftLoggerAdapter>();
        serviceCollection.AddTransient<IRouteNameExtractor, RouteNameExtractor>();
        serviceCollection.AddTransient<ITopicBaseTopicNameCreator, TopicBaseTopicNameCreator>();
        serviceCollection.AddTransient<IEssentialTopicNameCreator, EssentialTopicNameCreator>();
        serviceCollection.AddTransient<IEssentialRouteReaderFactory, EssentialRouteReaderFactory>();
        serviceCollection.AddTransient<IEssentialPacketsReaderConnectorService, EssentialPacketsReaderConnectorService>();
        serviceCollection.AddTransient<IEssentialReadRequestHandler, EssentialReadRequestHandler>();
        serviceCollection.AddTransient<IDataStreamWriterHandlerFactory, DataStreamWriterHandlerFactory>();
        serviceCollection.AddTransient<IIdentifierFilter, IdentifierFilter>();
        serviceCollection.AddTransient<IStreamWriterRepositoryFactory, StreamWriterRepositoryFactory>();
        serviceCollection.AddTransient<ISessionRouteSubscriberFactory, SessionRouteSubscriberFactory>();
        serviceCollection.AddTransient<IDataFormatRoutesFactory, DataFormatRoutesFactory>();
        serviceCollection.AddTransient<IDataFormatRouteSubscriberFactory, DataFormatRouteSubscriberFactory>();
        serviceCollection.AddTransient<IDtoFromByteFactory<PacketDto>, PacketDtoFromByteFactory>();
        serviceCollection.AddTransient<IDtoFromByteFactory<DataFormatDefinitionPacketDto>, DataFormatDefinitionPacketDtoFromByteFactory>();
        serviceCollection.AddTransient<IDtoFromByteFactory<SessionInfoPacketDto>, SessionInfoPacketDtoFromByteFactory>();
        serviceCollection.AddTransient<IDtoFromByteFactory<NewSessionPacketDto>, NewSessionPacketDtoFromByteFactory>();
        serviceCollection.AddTransient<IDtoFromByteFactory<EndOfSessionPacketDto>, EndOfSessionPacketDtoFromByteFactory>();
        serviceCollection.AddTransient<ITypeNameProvider, TypeNameProvider>();
        serviceCollection.AddTransient<IKafkaTopicHelper, KafkaTopicHelper>();
        serviceCollection.AddTransient<IParameterListKeyIdentifierCreator, ParameterListKeyIdentifierCreator>();
        serviceCollection.AddTransient<IServiceResolver, ServiceResolver>();

        if (this.streamingApiConfiguration.UseRemoteKeyGenerator)
        {
            serviceCollection.AddSingleton<IKeyGeneratorService, RemoteKeyGeneratorService>();
        }
        else
        {
            serviceCollection.AddSingleton<IKeyGeneratorService, KeyGeneratorService>();
        }

        if (this.streamingApiConfiguration.StreamCreationStrategy == StreamCreationStrategy.PartitionBased)
        {
            serviceCollection.AddSingleton<IRouterProvider, PartitionBasedRouterProvider>();
            serviceCollection.AddTransient<IRouteSubscriberFactory, PartitionBasedRouteSubscriberFactory>();
            serviceCollection.AddTransient<IRouteInfoProvider, PartitionBasedKafkaRouteInfoProvider>();
        }
        else
        {
            serviceCollection.AddSingleton<IRouterProvider, TopicBasedRouterProvider>();
            serviceCollection.AddTransient<IRouteSubscriberFactory, TopicBasedRouteSubscriberFactory>();
            serviceCollection.AddTransient<IRouteInfoProvider, TopicBasedKafkaRouteInfoProvider>();
        }

        if (registerServices)
        {
            serviceCollection.AddTransient<ConnectionManager>();
            serviceCollection.AddTransient<DataFormatManager>();
            serviceCollection.AddTransient<PacketWriter>();
            serviceCollection.AddTransient<PacketReader>();
            serviceCollection.AddTransient<SessionManager>();
        }
    }

    private static void ConfigureLogging(IServiceCollection services, Action<ILoggingBuilder> configureLogging)
    {
        services.AddLogging(configureLogging);
    }
}
