// <copyright file="DataFormatInfoServiceBase.cs" company="Motion Applied Ltd.">
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

using FluentAssertions;

using Google.Protobuf;

using MA.Common;
using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.DataFormatManagement;
using MA.Streaming.Core.Routing;
using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Core.Factories;
using MA.Streaming.Proto.Core.Providers;

using NSubstitute;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests.Base;

public abstract class DataFormatInfoServiceBase
{
    protected abstract string BrokerUrl { get; }

    protected abstract string DataSourcePrefix { get; }

    protected abstract string DomainPrefix { get; }

    protected virtual IKafkaSecurityConfigurationProvider CreateSecurityProvider() =>
        new EmptyKafkaSecurityConfigProvider();

    private readonly ITestOutputHelper outputHelper;
    private readonly Lazy<TestContext> context;
    private readonly ILogger logger;

    protected DataFormatInfoServiceBase(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
        this.context = new Lazy<TestContext>(this.CreateTestContext);
        this.logger = Substitute.For<ILogger>();
    }

    private TestContext CreateTestContext()
    {
        var dataSource = DataSourceCreator.Create(this.DataSourcePrefix);
        var domain1 = DomainCreator.Create(this.DomainPrefix);
        var security = this.CreateSecurityProvider();

        var streamingApiConfiguration = new StreamingApiConfiguration(
            StreamCreationStrategy.PartitionBased,
            this.BrokerUrl,
            [],
            integrateDataFormatManagement: true,
            integrateSessionManagement: false,
            domain: domain1);

        var repository = new RouteReadingWritingComponentRepository<KafkaRouteSubscriber>(this.logger);
        var streamingApiConfigurationProvider = new StreamingApiConfigurationProvider(streamingApiConfiguration);
        var routingMappingInfoRepository = new RoutingMappingInfoRepository();
        var domainNameProvider = new DomainNameProvider(domain1);
        var domainKafkaRouteMappingInfoCreator = new DomainKafkaRouteMappingInfoCreator(domainNameProvider, streamingApiConfigurationProvider);
        var kafkaRouteInfoCreator = new KafkaRouteInfoCreator(domainKafkaRouteMappingInfoCreator, streamingApiConfigurationProvider, routingMappingInfoRepository);
        var dataFormatRoutesFactory = new DataFormatRoutesFactory(kafkaRouteInfoCreator);
        var kafkaTopicHelper = new KafkaTopicHelper(this.logger);
        var secu4SecurityProvider = new KafkaSecurityConfigurationProviderAdapter(security);
        var dataSourceRepository = new DataSourcesRepository(
            kafkaTopicHelper,
            streamingApiConfigurationProvider,
            domainKafkaRouteMappingInfoCreator,
            secu4SecurityProvider);
        var dataFormatRouteSubscriberFactory = new DataFormatRouteSubscriberFactory(
            streamingApiConfigurationProvider,
            this.logger,
            new KafkaRouteManager(this.logger),
            repository,
            domainNameProvider,
            secu4SecurityProvider);
        var stringToDataRecordRepository = new ThreadSafeInMemoryRepository<(string, string, DataFormatTypeDto), DataFormatRecord>();
        var ulongToDataRecordRepository = new ThreadSafeInMemoryRepository<(string, ulong, DataFormatTypeDto), DataFormatRecord>();
        var datasourceDataFormatsRepository = new ThreadSafeInMemoryRepository<string, List<DataFormatRecord>>();
        var packetDtoFromByteFactory = new PacketDtoFromByteFactory(this.logger);
        var parameterListKeyIdentifierCreator = new ParameterListKeyIdentifierCreator();
        var dataFormatDefinitionPacketDtoFromByteFactory = new DataFormatDefinitionPacketDtoFromByteFactory(this.logger, parameterListKeyIdentifierCreator);
        var typeNameProvider = new TypeNameProvider();

        var dataFormatInfoService = new DataFormatInfoService(
            streamingApiConfigurationProvider,
            dataFormatRoutesFactory,
            dataSourceRepository,
            dataFormatRouteSubscriberFactory,
            stringToDataRecordRepository,
            ulongToDataRecordRepository,
            datasourceDataFormatsRepository,
            packetDtoFromByteFactory,
            dataFormatDefinitionPacketDtoFromByteFactory,
            typeNameProvider,
            parameterListKeyIdentifierCreator,
            routingMappingInfoRepository,
            this.logger);
        var keyGenerator = new KeyGeneratorService(new LoggingDirectoryProvider(""));
        var essentialTopicName = kafkaRouteInfoCreator.CreateEssentialRouteInfo(dataSource).KafkaRouteTopicInfo.Topic;

        return new TestContext(
            dataSource,
            stringToDataRecordRepository,
            ulongToDataRecordRepository,
            dataFormatInfoService,
            dataSourceRepository,
            keyGenerator,
            typeNameProvider,
            essentialTopicName);
    }

    [Fact]
    public void Start_Properly_When_Broker_Is_Available()
    {
        var startingSuccess = false;
        try
        {
            this.context.Value.DataFormatInfoService.Start();
            startingSuccess = true;
        }
        catch (Exception ex)
        {
            this.outputHelper.WriteLine($"exception: {ex}");
        }

        startingSuccess.Should().BeTrue();
    }

    [Fact]
    public void Not_Created_Subscriber_On_Empty_Fresh_Broker()
    {
        var kafkaTopicHelper = new KafkaTopicHelper(this.logger);
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicPrefix(
            this.BrokerUrl,
            this.context.Value.EssentialTopicName,
            KafkaSecurityConfigurationMapper.Map(this.CreateSecurityProvider().Provide()));
        this.context.Value.DataFormatInfoService.Start();
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicPrefix(
            this.BrokerUrl,
            this.context.Value.EssentialTopicName,
            KafkaSecurityConfigurationMapper.Map(this.CreateSecurityProvider().Provide()));
        beforeStartTopicInfos.Count.Should().Be(0);
        afterStartTopicInfos.Count.Should().Be(0);
        this.context.Value.DataFormatInfoService.RouteSubscribers.Count.Should().Be(0);
    }

    [Fact]
    public void Create_Subscriber_After_DataSource_Added()
    {
        var kafkaTopicHelper = new KafkaTopicHelper(this.logger);
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicPrefix(
            this.BrokerUrl,
            this.context.Value.EssentialTopicName,
            KafkaSecurityConfigurationMapper.Map(this.CreateSecurityProvider().Provide()));
        this.context.Value.DataFormatInfoService.Start();
        this.context.Value.DataSourceRepository.Add(this.context.Value.DataSource);
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicPrefix(
            this.BrokerUrl,
            this.context.Value.EssentialTopicName,
            KafkaSecurityConfigurationMapper.Map(this.CreateSecurityProvider().Provide()));
        beforeStartTopicInfos.Count.Should().Be(0);
        afterStartTopicInfos.Count.Should().Be(1);
        this.context.Value.DataFormatInfoService.RouteSubscribers.Count.Should().Be(1);
    }

    [Fact]
    public void Create_Subscriber_For_Existed_DataSource()
    {
        var kafkaTopicHelper = new KafkaTopicHelper(this.logger);
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicPrefix(
            this.BrokerUrl,
            this.context.Value.EssentialTopicName,
            KafkaSecurityConfigurationMapper.Map(this.CreateSecurityProvider().Provide()));
        this.context.Value.DataSourceRepository.Add(this.context.Value.DataSource);
        this.context.Value.DataFormatInfoService.Start();
        new AutoResetEvent(false).WaitOne(1000);
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicPrefix(
            this.BrokerUrl,
            this.context.Value.EssentialTopicName,
            KafkaSecurityConfigurationMapper.Map(this.CreateSecurityProvider().Provide()));
        beforeStartTopicInfos.Count.Should().Be(0);
        afterStartTopicInfos.Count.Should().Be(1);
        this.context.Value.DataFormatInfoService.RouteSubscribers.Count.Should().Be(1);
    }

    [Fact]
    public void Read_All_Exist_Data_Format_Related_Packets_And_Update_The_Repository()
    {
        var security = KafkaSecurityConfigurationMapper.Map(this.CreateSecurityProvider().Provide());
        var kafkaPublishHelper = new KafkaPublishHelper(this.BrokerUrl, security);
        var kafkaTopicHelper = new KafkaTopicHelper(this.logger);
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicPrefix(this.BrokerUrl, this.context.Value.EssentialTopicName, security);
        var paramUlongIdentifier = this.context.Value.KeyGenerator.GenerateUlongKey();
        var parameterIdentifiersList = new List<string>
        {
            "Param1",
            "Param2",
            "Param3"
        };
        var parameterDataFormatDefinitionPacket = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Parameter,
            ParameterIdentifiers = new ParameterList
            {
                ParameterIdentifiers =
                {
                    parameterIdentifiersList
                }
            },
            Identifier = paramUlongIdentifier
        };
        var eventUlongIdentifier = this.context.Value.KeyGenerator.GenerateUlongKey();
        const string EventIdentifier = "event1";
        var eventDataFormatDefinitionPacket = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Event,
            EventIdentifier = EventIdentifier,
            Identifier = eventUlongIdentifier
        };

        kafkaPublishHelper.PublishData(
            this.context.Value.EssentialTopicName,
            GetPacketBytes(this.context.Value.TypeNameProvider.DataFormatDefinitionPacketTypeName, parameterDataFormatDefinitionPacket.ToByteString()));
        kafkaPublishHelper.PublishData(
            this.context.Value.EssentialTopicName,
            GetPacketBytes(this.context.Value.TypeNameProvider.DataFormatDefinitionPacketTypeName, eventDataFormatDefinitionPacket.ToByteString()));
        new AutoResetEvent(false).WaitOne(2000);

        this.context.Value.DataFormatInfoService.Start();
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicPrefix(this.BrokerUrl, this.context.Value.EssentialTopicName, security);

        beforeStartTopicInfos.Any(i => i.TopicName == this.context.Value.EssentialTopicName).Should().BeFalse();
        afterStartTopicInfos.Any(i => i.TopicName == this.context.Value.EssentialTopicName).Should().BeTrue();
        var allDataFormatsRecords = this.context.Value.StringToDataRecordRepository.GetAll();
        allDataFormatsRecords.Count.Should().Be(2);
        allDataFormatsRecords.Count(i => i.DataFormatTypeDto == DataFormatTypeDto.Parameter).Should().Be(1);
        var parametersDataFormatRecord = allDataFormatsRecords.First(i => i.DataFormatTypeDto == DataFormatTypeDto.Parameter);
        parametersDataFormatRecord.StringIdentifiers.Should().BeEquivalentTo(parameterIdentifiersList);
        const string ExpectedKey = "Param1,Param2,Param3";
        parametersDataFormatRecord.StringIdentifierKey.Should().Be(ExpectedKey);
        parametersDataFormatRecord.Identifiers.Should().BeEquivalentTo([paramUlongIdentifier]);

        allDataFormatsRecords.Count(i => i.DataFormatTypeDto == DataFormatTypeDto.Event).Should().Be(1);
        var eventRecord = allDataFormatsRecords.First(i => i.DataFormatTypeDto == DataFormatTypeDto.Event);
        eventRecord.StringIdentifiers.Should().BeEquivalentTo(
            new List<string>
            {
                EventIdentifier
            });
        eventRecord.StringIdentifierKey.Should().Be(EventIdentifier);
        eventRecord.Identifiers.Should().BeEquivalentTo([eventUlongIdentifier]);

        this.context.Value.StringToDataRecordRepository.Get((this.context.Value.DataSource, ExpectedKey, DataFormatTypeDto.Parameter)).Should().NotBeNull();
        this.context.Value.UlongToDataRecordRepository.Get((this.context.Value.DataSource, paramUlongIdentifier, DataFormatTypeDto.Parameter)).Should().NotBeNull();
        this.context.Value.StringToDataRecordRepository.Get((this.context.Value.DataSource, EventIdentifier, DataFormatTypeDto.Event)).Should().NotBeNull();
        this.context.Value.UlongToDataRecordRepository.Get((this.context.Value.DataSource, eventUlongIdentifier, DataFormatTypeDto.Event)).Should().NotBeNull();
    }

    [Fact]
    public void Add_Identifiers_To_Exist_DataFormat_Records_Identifier_Exist_Data_Format_Related_Packets_And_Update_The_Repository()
    {
        var security = KafkaSecurityConfigurationMapper.Map(this.CreateSecurityProvider().Provide());
        var kafkaPublishHelper = new KafkaPublishHelper(this.BrokerUrl, security);
        var kafkaTopicHelper = new KafkaTopicHelper(this.logger);
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicPrefix(this.BrokerUrl, this.context.Value.EssentialTopicName, security);
        var paramUlongIdentifier1 = this.context.Value.KeyGenerator.GenerateUlongKey();
        var paramUlongIdentifier2 = this.context.Value.KeyGenerator.GenerateUlongKey();
        var parameterIdentifiersList = new List<string>
        {
            "Param1",
            "Param2",
            "Param3"
        };
        var paramPacket1 = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Parameter,
            ParameterIdentifiers = new ParameterList
            {
                ParameterIdentifiers =
                {
                    parameterIdentifiersList
                }
            },
            Identifier = paramUlongIdentifier1
        };
        var paramPacket2 = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Parameter,
            ParameterIdentifiers = new ParameterList
            {
                ParameterIdentifiers =
                {
                    parameterIdentifiersList
                }
            },
            Identifier = paramUlongIdentifier2
        };
        var eventUlongIdentifier1 = this.context.Value.KeyGenerator.GenerateUlongKey();
        var eventUlongIdentifier2 = this.context.Value.KeyGenerator.GenerateUlongKey();
        var eventIdentifier = "event1";
        var eventPacket1 = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Event,
            EventIdentifier = eventIdentifier,
            Identifier = eventUlongIdentifier1
        };
        var eventPacket2 = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Event,
            EventIdentifier = eventIdentifier,
            Identifier = eventUlongIdentifier2
        };

        kafkaPublishHelper.PublishData(
            this.context.Value.EssentialTopicName,
            GetPacketBytes(this.context.Value.TypeNameProvider.DataFormatDefinitionPacketTypeName, paramPacket1.ToByteString()));
        kafkaPublishHelper.PublishData(
            this.context.Value.EssentialTopicName,
            GetPacketBytes(this.context.Value.TypeNameProvider.DataFormatDefinitionPacketTypeName, eventPacket1.ToByteString()));
        kafkaPublishHelper.PublishData(
            this.context.Value.EssentialTopicName,
            GetPacketBytes(this.context.Value.TypeNameProvider.DataFormatDefinitionPacketTypeName, paramPacket2.ToByteString()));
        kafkaPublishHelper.PublishData(
            this.context.Value.EssentialTopicName,
            GetPacketBytes(this.context.Value.TypeNameProvider.DataFormatDefinitionPacketTypeName, eventPacket2.ToByteString()));
        new AutoResetEvent(false).WaitOne(2000);

        this.context.Value.DataFormatInfoService.Start();
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicPrefix(this.BrokerUrl, this.context.Value.EssentialTopicName, security);

        beforeStartTopicInfos.Any(i => i.TopicName == this.context.Value.EssentialTopicName).Should().BeFalse();
        afterStartTopicInfos.Any(i => i.TopicName == this.context.Value.EssentialTopicName).Should().BeTrue();
        var allDataFormatsRecords = this.context.Value.StringToDataRecordRepository.GetAll();
        allDataFormatsRecords.Count.Should().Be(2);
        var parametersDataFormatRecord = allDataFormatsRecords.First(i => i.DataFormatTypeDto == DataFormatTypeDto.Parameter);
        parametersDataFormatRecord.Identifiers.Should().BeEquivalentTo([paramUlongIdentifier1, paramUlongIdentifier2]);
        var eventRecord = allDataFormatsRecords.First(i => i.DataFormatTypeDto == DataFormatTypeDto.Event);
        eventRecord.Identifiers.Should().BeEquivalentTo([eventUlongIdentifier1, eventUlongIdentifier2]);
    }

    private static byte[] GetPacketBytes(string packetType, ByteString content) =>
        new Packet
        {
            Content = content,
            IsEssential = false,
            SessionKey = "",
            Type = packetType
        }.ToByteArray();

    private sealed record TestContext(
        string DataSource,
        ThreadSafeInMemoryRepository<(string, string, DataFormatTypeDto), DataFormatRecord> StringToDataRecordRepository,
        ThreadSafeInMemoryRepository<(string, ulong, DataFormatTypeDto), DataFormatRecord> UlongToDataRecordRepository,
        DataFormatInfoService DataFormatInfoService,
        DataSourcesRepository DataSourceRepository,
        KeyGeneratorService KeyGenerator,
        TypeNameProvider TypeNameProvider,
        string EssentialTopicName);
}
