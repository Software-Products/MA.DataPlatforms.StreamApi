// <copyright file="SessionInfoServiceShould.cs" company="Motion Applied Ltd.">
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
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;
using MA.Streaming.Abstraction;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.Core.SessionManagement;
using MA.Streaming.IntegrationTests.Base;
using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.OpenData;
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core.Factories;
using MA.Streaming.Proto.Core.Providers;

using NSubstitute;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class SessionInfoServiceShould 
{
    private const string BrokerUrl = "localhost:9098";
    private readonly ITestOutputHelper outputHelper;
    private readonly SessionInfoService sessionInfoService;
    private readonly ThreadSafeInMemoryRepository<string, SessionDetailRecord> memoryRepository;
    private readonly TypeNameProvider typeNameProvider;
    private readonly string sessionInfoTopicName;
    private readonly string mainTopicName;
    private readonly string essentialTopicName;


    public SessionInfoServiceShould(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
        this.memoryRepository = new ThreadSafeInMemoryRepository<string, SessionDetailRecord>();

        var domainPrefix = "SessionInfoDomain";
        var domain = DomainCreator.Create(domainPrefix);
        var apiConfigurationProvider =
            new StreamingApiConfigurationProvider(
                new StreamingApiConfiguration(
                    StreamCreationStrategy.PartitionBased,
                    BrokerUrl,
                    [],
                    integrateDataFormatManagement: false,
                    integrateSessionManagement: true,
                    domain: domain));
        var logger = Substitute.For<ILogger>();
        var repository = new RouteReadingWritingComponentRepository<KafkaRouteSubscriber>(logger);
        var kafkaRouteManager = new KafkaRouteManager(logger);
        var routingMappingInfoRepository = new RoutingMappingInfoRepository();
        var domainNameProvider = new DomainNameProvider(domain);
        var domainKafkaRouteMappingInfoCreator = new DomainKafkaRouteMappingInfoCreator(domainNameProvider, apiConfigurationProvider);
        var kafkaRouteInfoCreator = new KafkaRouteInfoCreator(domainKafkaRouteMappingInfoCreator, apiConfigurationProvider, routingMappingInfoRepository);
        var sessionRouteSubscriberFactory = new SessionRouteSubscriberFactory(
            apiConfigurationProvider,
            kafkaRouteManager,
            kafkaRouteInfoCreator,
            logger,
            repository);
        var packetDtoFromByteFactory = new PacketDtoFromByteFactory(logger);
        var newSessionDtoFromByteFactory = new NewSessionPacketDtoFromByteFactory(logger);
        var endOfSessionDtoFromByteFactory = new EndOfSessionPacketDtoFromByteFactory(logger);
        var sessionInfoDtoFromByteFactory = new SessionInfoPacketDtoFromByteFactory(logger);
        var datasourceRepository = Substitute.For<IDataSourcesRepository>();
        this.typeNameProvider = new TypeNameProvider();
        this.sessionInfoService = new SessionInfoService(
            apiConfigurationProvider,
            this.memoryRepository,
            sessionRouteSubscriberFactory,
            logger,
            packetDtoFromByteFactory,
            sessionInfoDtoFromByteFactory,
            newSessionDtoFromByteFactory,
            endOfSessionDtoFromByteFactory,
            this.typeNameProvider,
            kafkaRouteInfoCreator,
            datasourceRepository);

        this.sessionInfoTopicName = kafkaRouteInfoCreator.CreateSessionInfoRouteInfo().KafkaRouteTopicInfo.Topic;
        this.mainTopicName = kafkaRouteInfoCreator.CreateMainRouteInfo("Session_Info_Test_Datasource").KafkaRouteTopicInfo.Topic;
        this.essentialTopicName = kafkaRouteInfoCreator.CreateEssentialRouteInfo("Session_Info_Test_Datasource").KafkaRouteTopicInfo.Topic;
    }

    [Fact]
    public void Start_Properly_When_Broker_Is_Available()
    {
        //arrange
        var startingSuccess = false;

        //act
        try
        {
            this.sessionInfoService.ServiceStarted += (_, _) =>
            {
                startingSuccess = true;
            };
            this.sessionInfoService.Start();
        }
        catch (Exception ex)
        {
            this.outputHelper.WriteLine($"exception happened in session info service starting. exception {ex}");
        }

        //assert
        startingSuccess.Should().BeTrue();
    }

    [Fact]
    public void Create_Session_Info_Topic_At_Starting_When_Not_Exist()
    {
        //arrange
        var kafkaTopicHelper = new KafkaTopicHelper();
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(BrokerUrl, this.sessionInfoTopicName);

        //act

        this.sessionInfoService.Start();
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(BrokerUrl, this.sessionInfoTopicName);

        //assert
        beforeStartTopicInfos.Any(i => i.TopicName == this.sessionInfoTopicName).Should().BeFalse();
        afterStartTopicInfos.Any(i => i.TopicName == this.sessionInfoTopicName).Should().BeTrue();
        var sessionInfoTopicInfos = afterStartTopicInfos.Where(i => i.TopicName == this.sessionInfoTopicName).ToList();
        sessionInfoTopicInfos.Count.Should().Be(1);
        sessionInfoTopicInfos[0].Partition.Should().Be(0);
        sessionInfoTopicInfos[0].Offset.Should().Be(0);
    }

    [Fact]
    public void Read_All_Exist_Session_Related_Packets_And_Update_The_Repository()
    {
        //arrange
        var kafkaPublishHelper = new KafkaPublishHelper(BrokerUrl);
        var kafkaTopicHelper = new KafkaTopicHelper();
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(BrokerUrl, this.sessionInfoTopicName);
        var serviceStarted = false;
        var serviceStopped = false;

        this.sessionInfoService.ServiceStarted += (_, _) => serviceStarted = true;
        this.sessionInfoService.ServiceStopped += (_, _) => serviceStopped = true;

        const string DataSource = "Session_Info_Test_Datasource";
        MetricProviders.NumberOfSessions.WithLabels(DataSource).Set(0);
        const long ExpectedMainOffset = 55L;
        const long ExpectedEssentialOffset = 77L;
        var mapField = new MapField<string, long>
        {
            { $"{DataSource}:{0}", 10 },
            { $"{this.mainTopicName}:{0}", ExpectedMainOffset },
            { $"{this.essentialTopicName}:{0}", ExpectedEssentialOffset }
        };
        var sessionKey = Guid.NewGuid().ToString();
        var newSessionPacket = new NewSessionPacket
        {
            DataSource = DataSource,
            TopicPartitionOffsets =
            {
                mapField
            },
            UtcOffset = Duration.FromTimeSpan(TimeSpan.FromHours(-2))
        };

        var sessionInfoPacketName = this.typeNameProvider.SessionInfoPacketTypeName;
        const uint Version = 1;
        var newSessionInfoPacket = new SessionInfoPacket
        {
            Type = sessionInfoPacketName,
            DataSource = DataSource,
            Version = Version,
        };

        const string NewIdentifier = "new_identifier";
        var updateSessionIdentifierPacket = new SessionInfoPacket
        {
            Identifier = NewIdentifier,
        };

        var associatedId1 = Guid.NewGuid().ToString();
        var associatedId2 = Guid.NewGuid().ToString();

        var addAssociateSessionPacket1 = new SessionInfoPacket
        {
            AssociateSessionKeys =
            {
                associatedId1
            },
        };

        var addAssociateSessionPacket2 = new SessionInfoPacket
        {
            AssociateSessionKeys =
            {
                associatedId1,
                associatedId2
            },
        };

        var endSessionPacket = new EndOfSessionPacket
        {
            DataSource = DataSource
        };

        kafkaPublishHelper.PublishData(
            this.sessionInfoTopicName,
            GetPacketBytes(sessionKey, this.typeNameProvider.NewSessionPacketTypeName, newSessionPacket.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.sessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, newSessionInfoPacket.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.sessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, updateSessionIdentifierPacket.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.sessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, addAssociateSessionPacket1.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.sessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.sessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.sessionInfoTopicName,
            GetPacketBytes(sessionKey, this.typeNameProvider.EndOfSessionPacketTypeName, endSessionPacket.ToByteString()),
            sessionKey);

        //act
        this.sessionInfoService.Start();
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(BrokerUrl, this.sessionInfoTopicName);
        //assert
        serviceStarted.Should().BeTrue();
        serviceStopped.Should().BeFalse();
        beforeStartTopicInfos.Any(i => i.TopicName == this.sessionInfoTopicName).Should().BeFalse();
        afterStartTopicInfos.Any(i => i.TopicName == this.sessionInfoTopicName).Should().BeTrue();
        var sessions = this.memoryRepository.GetAll(i => i.DataSource == DataSource);
        sessions.Count.Should().Be(1);
        sessions[0].SessionKey.Should().Be(sessionKey);
        sessions[0].Completed.Should().BeTrue();
        sessions[0].StartingOffsetInfo.Count.Should().Be(3);
        sessions[0].StartingOffsetInfo[0].TopicName.Should().Be(DataSource);
        sessions[0].StartingOffsetInfo[0].Partition.Should().Be(0);
        sessions[0].StartingOffsetInfo[0].Offset.Should().Be(10);
        sessions[0].MainOffset.Should().Be(
            ExpectedMainOffset,
            "MainOffset must be resolved via the main topic name produced by kafkaRouteInfoCreator.CreateMainRouteInfo(), not the raw DataSource string");
        sessions[0].EssentialOffset.Should().Be(
            ExpectedEssentialOffset,
            "EssentialOffset must be resolved via the essential topic name produced by kafkaRouteInfoCreator.CreateEssentialRouteInfo()");
        sessions[0].SessionInfoPacket.Identifier.Should().Be(NewIdentifier);
        sessions[0].SessionInfoPacket.AssociatedKeys.Should().BeEquivalentTo(
            new List<string>
            {
                associatedId1,
                associatedId2
            });
        sessions[0].SessionInfoPacket.Type.Should().Be(sessionInfoPacketName);
        sessions[0].SessionInfoPacket.Version.Should().Be(Version);
        MetricProviders.NumberOfSessions.WithLabels(DataSource).Value.Should().Be(1);

        //act
        this.sessionInfoService.Stop();

        //assert
        serviceStopped.Should().BeTrue();
    }

    private static byte[] GetPacketBytes(string sessionKey, string packetType, ByteString content)
    {
        return new Packet
        {
            Content = content,
            IsEssential = false,
            SessionKey = sessionKey,
            Type = packetType
        }.ToByteArray();
    }
}
