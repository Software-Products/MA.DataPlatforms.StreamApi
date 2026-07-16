// <copyright file="ServiceConfigurator.cs" company="Motion Applied Ltd.">
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

using MA.Common;
using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.DataPlatforms.Secu4.RouterComponent;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent;
using MA.DataPlatforms.Secu4.Routing.Configuration.Core;
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
    private readonly ILogger? logger;
    private readonly IKafkaSecurityConfigProvider kafkaSecurityConfigProvider;

    public ServiceConfigurator(
        IStreamingApiConfiguration streamingApiConfiguration,
        ICancellationTokenSourceProvider cancellationTokenSourceProvider,
        ILoggingDirectoryProvider loggingDirectoryProvider,
        ILogger? logger,
        IKafkaSecurityConfigurationProvider? kafkaSecurityConfigProvider)
    {
        this.streamingApiConfiguration = streamingApiConfiguration;
        this.cancellationTokenSourceProvider = cancellationTokenSourceProvider;
        this.loggingDirectoryProvider = loggingDirectoryProvider;
        this.logger = logger;
        this.kafkaSecurityConfigProvider = new KafkaSecurityConfigurationProviderAdapter(kafkaSecurityConfigProvider, logger);
    }

    public void Configure(IServiceCollection serviceCollection, bool registerServices)
    {
        ConfigureLogging(serviceCollection, this.logger);
        serviceCollection.AddSingleton<IStreamingApiConfigurationProvider>(new StreamingApiConfigurationProvider(this.streamingApiConfiguration));
        serviceCollection.AddSingleton(this.kafkaSecurityConfigProvider);
        serviceCollection.AddSingleton<IRouteManager, KafkaRouteManager>();
        serviceCollection.AddSingleton<IActiveConnectionManager, ActiveConnectionManager>();
        serviceCollection.AddSingleton<IDomainNameProvider>(new DomainNameProvider(this.streamingApiConfiguration.Domain));
        serviceCollection.AddSingleton<IStreamWriterHandlerFactory, StreamWriterHandlerFactory>();
        serviceCollection.AddSingleton(this.cancellationTokenSourceProvider);
        serviceCollection.AddSingleton<IInMemoryRepository<string, SessionDetailRecord>, ThreadSafeInMemoryRepository<string, SessionDetailRecord>>();
        serviceCollection.AddSingleton<IInMemoryRepository<string, List<DataFormatRecord>>, ThreadSafeInMemoryRepository<string, List<DataFormatRecord>>>();
        serviceCollection.AddSingleton<IRouteReadingWritingComponentRepository<KafkaRouter>, RouteReadingWritingComponentRepository<KafkaRouter>>();
        serviceCollection.AddSingleton<IRouteReadingWritingComponentRepository<KafkaRouteSubscriber>, RouteReadingWritingComponentRepository<KafkaRouteSubscriber>>();
        serviceCollection.AddSingleton<IDataFormatInfoService, DataFormatInfoService>();
        serviceCollection.AddSingleton<ISessionInfoService, SessionInfoService>();
        serviceCollection.AddSingleton<IStreamReaderHandlerFactory, StreamReaderHandlerFactory>();
        serviceCollection.AddSingleton<IDataSourcesRepository, DataSourcesRepository>();
        serviceCollection.AddSingleton<ISessionNotificationManagerService, SessionNotificationManagerService>();
        serviceCollection.AddSingleton<IDomainKafkaRouteMappingInfoCreator, DomainKafkaRouteMappingInfoCreator>();
        serviceCollection.AddSingleton<IKafkaRouteInfoCreator, KafkaRouteInfoCreator>();
        serviceCollection.AddSingleton<IKafkaBrokerRoutesOffsetExtractor, KafkaBrokerRoutesOffsetExtractor>();
        serviceCollection.AddSingleton<IRoutingMappingInfoRepository, RoutingMappingInfoRepository>();
        serviceCollection.AddSingleton<IEssentialRouterProvider, EssentialRouterProvider>();
        serviceCollection.AddSingleton<IDataRouterProvider, DataRouterProvider>();
        serviceCollection.AddSingleton<IEssentialRouteReaderFactory, EssentialRouteReaderFactory>();
        serviceCollection.AddSingleton<IEssentialRouterConfigurationProvider, EssentialRouterConfigurationProvider>();
        serviceCollection.AddTransient<IEssentialReadRequestHandlerFactory, EssentialReadRequestHandlerFactory>();
        serviceCollection.AddSingleton<IRouterProvider, RouterProvider>();
        serviceCollection.AddSingleton<IRouteSubscriberFactory, RouteSubscriberFactory>();
        serviceCollection.AddSingleton<IRoutingConfigurationProviderCreator, RoutingConfigurationProviderCreator>();
        serviceCollection.AddSingleton<IDomainRouterProvider, DomainRouterProvider>();
        serviceCollection.AddSingleton<IDataSourceRoutingInfoTracker, DataSourceRoutingInfoTracker>();
        serviceCollection.AddSingleton<IConsumingConfigurationProviderCreator, ConsumingConfigurationProviderCreator>();
        serviceCollection.AddSingleton<IStreamStopPacketWriterTracker, StreamStopPacketWriterTracker>();

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

        serviceCollection.AddSingleton<IStreamStartedPacketRequestFactory, StreamStartedPacketRequestFactory>();
        serviceCollection.AddSingleton<IStreamStoppedPacketRequestFactory, StreamStoppedPacketRequestFactory>();
        serviceCollection.AddSingleton<ISessionPacketWritingLogProvider, SessionPacketWritingLogProvider>();
        serviceCollection.AddSingleton<IStreamManager, StreamManager>();
        serviceCollection.AddSingleton<IPacketWriterConnectorService, PacketWriterConnectorService>();
        serviceCollection.AddSingleton<IStreamsProvider, StreamsProvider>();
        serviceCollection.AddSingleton<ISessionStreamRoutingPacketTracker, SessionStreamRoutingPacketTracker>();

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
        serviceCollection.AddTransient<IEssentialPacketsReaderConnectorService, EssentialPacketsReaderConnectorService>();
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

        if (!registerServices)
        {
            return;
        }

        serviceCollection.AddTransient<ConnectionManager>();
        serviceCollection.AddTransient<DataFormatManager>();
        serviceCollection.AddTransient<PacketWriter>();
        serviceCollection.AddTransient<PacketReader>();
        serviceCollection.AddTransient<SessionManager>();
    }

    private static void ConfigureLogging(IServiceCollection services, ILogger? customLogger)
    {
        if (customLogger != null)
        {
            services.AddSingleton(customLogger);
        }
        else
        {
            services.AddLogging(loggingBuilder =>
            {
                loggingBuilder.ClearProviders();
                loggingBuilder.AddConsole();
                loggingBuilder.SetMinimumLevel(LogLevel.Warning);
            });
            services.AddTransient<ILogger, MicrosoftLoggerAdapter>();
        }
    }
}

