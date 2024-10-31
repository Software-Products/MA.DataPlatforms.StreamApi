// <copyright file="DataFormatInfoServiceShould.cs" company="McLaren Applied Ltd.">
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

using FluentAssertions;

using Google.Protobuf;

using MA.Common;
using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.DataFormatManagement;
using MA.Streaming.Core.Routing;
using MA.Streaming.Core.Routing.EssentialsRouting;
using MA.Streaming.IntegrationTests.Base;
using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Core.Factories;
using MA.Streaming.Proto.Core.Providers;

using NSubstitute;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class DataFormatInfoServiceShould : IClassFixture<KafkaTestsCleanUpFixture>
{
    private const string BrokerUrl = "localhost:9097";
    private readonly ITestOutputHelper outputHelper;
    private const string DataSource = "Data_Format_Info_Test_Datasource";

    private readonly ThreadSafeInMemoryRepository<(string, string, DataFormatTypeDto), DataFormatRecord> stringToDataRecordRepository;
    private readonly ThreadSafeInMemoryRepository<(string, ulong, DataFormatTypeDto), DataFormatRecord> ulongToDataRecordRepository;
    private readonly DataFormatInfoService dataFormatInfoService;
    private readonly DataSourcesRepository dataSourceRepository;
    private readonly KeyGeneratorService keyGenerator;
    private readonly EssentialTopicNameCreator essentialTopicNameCreator;
    private readonly TypeNameProvider typeNameProvider = new();

    public DataFormatInfoServiceShould(ITestOutputHelper outputHelper, KafkaTestsCleanUpFixture _)
    {
        this.outputHelper = outputHelper;
        var streamingApiConfiguration = new StreamingApiConfiguration(
            StreamCreationStrategy.PartitionBased,
            BrokerUrl,
            [],
            integrateDataFormatManagement: true,
            integrateSessionManagement: false);
        var logger = Substitute.For<ILogger>();
        var cancellationTokenSourceProvider = new CancellationTokenSourceProvider();
        var streamingApiConfigurationProvider =
            new StreamingApiConfigurationProvider(streamingApiConfiguration);
        var dataFormatRoutesFactory = new DataFormatRoutesFactory(new EssentialTopicNameCreator());
        this.dataSourceRepository = new DataSourcesRepository(new KafkaTopicHelper(), streamingApiConfigurationProvider);
        var dataFormatRouteSubscriberFactory = new DataFormatRouteSubscriberFactory(streamingApiConfigurationProvider, cancellationTokenSourceProvider, logger);
        this.stringToDataRecordRepository = new ThreadSafeInMemoryRepository<ValueTuple<string, string, DataFormatTypeDto>, DataFormatRecord>();
        this.ulongToDataRecordRepository = new ThreadSafeInMemoryRepository<ValueTuple<string, ulong, DataFormatTypeDto>, DataFormatRecord>();
        var datasourceDataFormatsRepository = new ThreadSafeInMemoryRepository<string, List<DataFormatRecord>>();
        var packetDtoFromByteFactory = new PacketDtoFromByteFactory(logger);
        var dataFormatDefinitionPacketDtoFromByteFactory = new DataFormatDefinitionPacketDtoFromByteFactory(logger, new ParameterListKeyIdentifierCreator());

        this.dataFormatInfoService = new DataFormatInfoService(
            dataFormatRoutesFactory,
            this.dataSourceRepository,
            dataFormatRouteSubscriberFactory,
            this.stringToDataRecordRepository,
            this.ulongToDataRecordRepository,
            datasourceDataFormatsRepository,
            packetDtoFromByteFactory,
            dataFormatDefinitionPacketDtoFromByteFactory,
            this.typeNameProvider,
            logger);
        new KafkaClearHelper(BrokerUrl).Clear().Wait();
        this.keyGenerator = new KeyGeneratorService(new LoggingDirectoryProvider(""));
        this.essentialTopicNameCreator = new EssentialTopicNameCreator();
    }

    [Fact]
    public void Start_Properly_When_Broker_Is_Available()
    {
        //arrange
        var startingSuccess = false;

        //act
        try
        {
            this.dataFormatInfoService.Start();
            startingSuccess = true;
        }
        catch (Exception ex)
        {
            this.outputHelper.WriteLine($"exception happened in session info service starting. exception {ex}");
        }

        //assert
        startingSuccess.Should().BeTrue();
    }

    [Fact]
    public void Not_Created_Subscriber_On_Empty_Fresh_Broker()
    {
        //arrange
        var kafkaTopicHelper = new KafkaTopicHelper();
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicSuffix(BrokerUrl, Constants.EssentialTopicNameSuffix);

        //act

        this.dataFormatInfoService.Start();
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicSuffix(BrokerUrl, Constants.EssentialTopicNameSuffix);

        //assert
        beforeStartTopicInfos.Count.Should().Be(0);
        afterStartTopicInfos.Count.Should().Be(0);
        this.dataFormatInfoService.RouteSubscribers.Count.Should().Be(0);
    }

    [Fact]
    public void Create_Subscriber_After_DataSource_Added()
    {
        //arrange
        var kafkaTopicHelper = new KafkaTopicHelper();
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicSuffix(BrokerUrl, Constants.EssentialTopicNameSuffix);

        //act

        this.dataFormatInfoService.Start();
        this.dataSourceRepository.Add(DataSource);
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicSuffix(BrokerUrl, Constants.EssentialTopicNameSuffix);

        //assert
        beforeStartTopicInfos.Count.Should().Be(0);
        afterStartTopicInfos.Count.Should().Be(1);
        this.dataFormatInfoService.RouteSubscribers.Count.Should().Be(1);
    }

    [Fact]
    public void Create_Subscriber_For_Existed_DataSource()
    {
        //arrange
        var kafkaTopicHelper = new KafkaTopicHelper();
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicSuffix(BrokerUrl, Constants.EssentialTopicNameSuffix);
        this.dataSourceRepository.Add(DataSource);

        //act

        this.dataFormatInfoService.Start();
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicSuffix(BrokerUrl, Constants.EssentialTopicNameSuffix);

        //assert
        beforeStartTopicInfos.Count.Should().Be(0);
        afterStartTopicInfos.Count.Should().Be(1);
        this.dataFormatInfoService.RouteSubscribers.Count.Should().Be(1);
    }

    [Fact]
    public void Read_All_Exist_Data_Format_Related_Packets_And_Update_The_Repository()
    {
        //arrange
        var kafkaPublishHelper = new KafkaPublishHelper(BrokerUrl);
        var kafkaTopicHelper = new KafkaTopicHelper();
        var essentialTopic = this.essentialTopicNameCreator.Create(DataSource);
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicSuffix(BrokerUrl, Constants.EssentialTopicNameSuffix);
        var paramUlongIdentifier = this.keyGenerator.GenerateUlongKey();
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

        var eventUlongIdentifier = this.keyGenerator.GenerateUlongKey();
        const string EventIdentifier = "event1";
        var eventDataFormatDefinitionPacket = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Event,
            EventIdentifier = EventIdentifier,
            Identifier = eventUlongIdentifier
        };

        kafkaPublishHelper.PublishData(
            essentialTopic,
            GetPacketBytes(this.typeNameProvider.DataFormatDefinitionPacketTypeName, parameterDataFormatDefinitionPacket.ToByteString()));
        kafkaPublishHelper.PublishData(
            essentialTopic,
            GetPacketBytes(this.typeNameProvider.DataFormatDefinitionPacketTypeName, eventDataFormatDefinitionPacket.ToByteString()));
        new AutoResetEvent(false).WaitOne(2000);
        //act
        this.dataFormatInfoService.Start();
        new AutoResetEvent(false).WaitOne(5000);
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicSuffix(BrokerUrl, Constants.EssentialTopicNameSuffix);

        //assert
        beforeStartTopicInfos.Any(i => i.TopicName == Constants.SessionInfoTopicName).Should().BeFalse();
        afterStartTopicInfos.Any(i => i.TopicName == essentialTopic).Should().BeTrue();
        var allDataFormatsRecords = this.stringToDataRecordRepository.GetAll();
        allDataFormatsRecords.Count.Should().Be(2);
        allDataFormatsRecords.Count(i => i.DataFormatTypeDto == DataFormatTypeDto.Parameter).Should().Be(1);
        var parametersDataFormatRecord = allDataFormatsRecords.First(i => i.DataFormatTypeDto == DataFormatTypeDto.Parameter);
        parametersDataFormatRecord.ParametersIdentifiers.Should().BeEquivalentTo(parameterIdentifiersList);
        const string ExpectedParameterIdentifiersKey = "Param1,Param2,Param3";
        parametersDataFormatRecord.ParametersIdentifierKey.Should().Be(ExpectedParameterIdentifiersKey);
        parametersDataFormatRecord.Identifiers.Should().BeEquivalentTo([paramUlongIdentifier]);

        allDataFormatsRecords.Count(i => i.DataFormatTypeDto == DataFormatTypeDto.Event).Should().Be(1);
        var eventRecord = allDataFormatsRecords.First(i => i.DataFormatTypeDto == DataFormatTypeDto.Event);
        eventRecord.ParametersIdentifiers.Should().BeEquivalentTo(
            new List<string>
            {
                EventIdentifier
            });
        eventRecord.ParametersIdentifierKey.Should().Be(EventIdentifier);
        eventRecord.Identifiers.Should().BeEquivalentTo([eventUlongIdentifier]);

        var foundDataFormatRecord = this.stringToDataRecordRepository.Get((DataSource, ExpectedParameterIdentifiersKey, DataFormatTypeDto.Parameter));
        foundDataFormatRecord.Should().NotBeNull();
        foundDataFormatRecord = this.ulongToDataRecordRepository.Get((DataSource, paramUlongIdentifier, DataFormatTypeDto.Parameter));
        foundDataFormatRecord.Should().NotBeNull();

        var foundEventDataFormatRecord = this.stringToDataRecordRepository.Get((DataSource, EventIdentifier, DataFormatTypeDto.Event));
        foundEventDataFormatRecord.Should().NotBeNull();
        foundEventDataFormatRecord = this.ulongToDataRecordRepository.Get((DataSource, eventUlongIdentifier, DataFormatTypeDto.Event));
        foundEventDataFormatRecord.Should().NotBeNull();
    }

    [Fact]
    public void Add_Identifiers_To_Exist_DataFormat_Records_Identifier_Exist_Data_Format_Related_Packets_And_Update_The_Repository()
    {
        //arrange
        var kafkaPublishHelper = new KafkaPublishHelper(BrokerUrl);
        var kafkaTopicHelper = new KafkaTopicHelper();
        var essentialTopic = this.essentialTopicNameCreator.Create(DataSource);
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicSuffix(BrokerUrl, Constants.EssentialTopicNameSuffix);
        var paramUlongIdentifier1 = this.keyGenerator.GenerateUlongKey();
        var paramUlongIdentifier2 = this.keyGenerator.GenerateUlongKey();
        var parameterIdentifiersList = new List<string>
        {
            "Param1",
            "Param2",
            "Param3"
        };
        var parameterDataFormatDefinitionPacket1 = new DataFormatDefinitionPacket
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

        var parameterDataFormatDefinitionPacket2 = new DataFormatDefinitionPacket
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

        var eventUlongIdentifier1 = this.keyGenerator.GenerateUlongKey();
        var eventUlongIdentifier2 = this.keyGenerator.GenerateUlongKey();
        var eventIdentifier = "event1";
        var eventDataFormatDefinitionPacket1 = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Event,
            EventIdentifier = eventIdentifier,
            Identifier = eventUlongIdentifier1
        };
        var eventDataFormatDefinitionPacket2 = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Event,
            EventIdentifier = eventIdentifier,
            Identifier = eventUlongIdentifier2
        };

        kafkaPublishHelper.PublishData(
            essentialTopic,
            GetPacketBytes(this.typeNameProvider.DataFormatDefinitionPacketTypeName, parameterDataFormatDefinitionPacket1.ToByteString()));
        kafkaPublishHelper.PublishData(
            essentialTopic,
            GetPacketBytes(this.typeNameProvider.DataFormatDefinitionPacketTypeName, eventDataFormatDefinitionPacket1.ToByteString()));
        kafkaPublishHelper.PublishData(
            essentialTopic,
            GetPacketBytes(this.typeNameProvider.DataFormatDefinitionPacketTypeName, parameterDataFormatDefinitionPacket2.ToByteString()));
        kafkaPublishHelper.PublishData(
            essentialTopic,
            GetPacketBytes(this.typeNameProvider.DataFormatDefinitionPacketTypeName, eventDataFormatDefinitionPacket2.ToByteString()));
        new AutoResetEvent(false).WaitOne(2000);
        //act
        this.dataFormatInfoService.Start();
        new AutoResetEvent(false).WaitOne(2000);
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicSuffix(BrokerUrl, Constants.EssentialTopicNameSuffix);

        //assert
        beforeStartTopicInfos.Any(i => i.TopicName == Constants.SessionInfoTopicName).Should().BeFalse();
        afterStartTopicInfos.Any(i => i.TopicName == essentialTopic).Should().BeTrue();
        var allDataFormatsRecords = this.stringToDataRecordRepository.GetAll();
        allDataFormatsRecords.Count.Should().Be(2);
        allDataFormatsRecords.Count(i => i.DataFormatTypeDto == DataFormatTypeDto.Parameter).Should().Be(1);
        var parametersDataFormatRecord = allDataFormatsRecords.First(i => i.DataFormatTypeDto == DataFormatTypeDto.Parameter);
        parametersDataFormatRecord.ParametersIdentifiers.Should().BeEquivalentTo(parameterIdentifiersList);
        const string ExpectedParameterIdentifiersKey = "Param1,Param2,Param3";
        parametersDataFormatRecord.ParametersIdentifierKey.Should().Be(ExpectedParameterIdentifiersKey);
        parametersDataFormatRecord.Identifiers.Should().BeEquivalentTo([paramUlongIdentifier1, paramUlongIdentifier2]);

        allDataFormatsRecords.Count(i => i.DataFormatTypeDto == DataFormatTypeDto.Event).Should().Be(1);
        var eventRecord = allDataFormatsRecords.First(i => i.DataFormatTypeDto == DataFormatTypeDto.Event);
        eventRecord.ParametersIdentifiers.Should().BeEquivalentTo([eventIdentifier]);
        eventRecord.ParametersIdentifierKey.Should().Be(eventIdentifier);
        eventRecord.Identifiers.Should().BeEquivalentTo([eventUlongIdentifier1, eventUlongIdentifier2]);

        var foundDataFormatRecord = this.stringToDataRecordRepository.Get((DataSource, ExpectedParameterIdentifiersKey, DataFormatTypeDto.Parameter));
        foundDataFormatRecord.Should().NotBeNull();
        foundDataFormatRecord = this.ulongToDataRecordRepository.Get((DataSource, paramUlongIdentifier1, DataFormatTypeDto.Parameter));
        foundDataFormatRecord.Should().NotBeNull();
        foundDataFormatRecord = this.ulongToDataRecordRepository.Get((DataSource, paramUlongIdentifier2, DataFormatTypeDto.Parameter));
        foundDataFormatRecord.Should().NotBeNull();

        var foundEventDataFormatRecord = this.stringToDataRecordRepository.Get((DataSource, eventIdentifier, DataFormatTypeDto.Event));
        foundEventDataFormatRecord.Should().NotBeNull();
        foundEventDataFormatRecord = this.ulongToDataRecordRepository.Get((DataSource, eventUlongIdentifier1, DataFormatTypeDto.Event));
        foundEventDataFormatRecord.Should().NotBeNull();
        foundEventDataFormatRecord = this.ulongToDataRecordRepository.Get((DataSource, eventUlongIdentifier2, DataFormatTypeDto.Event));
        foundEventDataFormatRecord.Should().NotBeNull();
    }

    private static byte[] GetPacketBytes(string packetType, ByteString content)
    {
        return new Packet
        {
            Content = content,
            IsEssential = false,
            SessionKey = "",
            Type = packetType
        }.ToByteArray();
    }
}
