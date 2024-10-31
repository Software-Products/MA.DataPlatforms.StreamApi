// <copyright file="SessionInfoServiceShould.cs" company="McLaren Applied Ltd.">
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
using Google.Protobuf.Collections;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.Streaming.Abstraction;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.Core.Routing.EssentialsRouting;
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
public class SessionInfoServiceShould : IClassFixture<KafkaTestsCleanUpFixture>
{
    private const string BrokerUrl = "localhost:9097";
    private readonly ITestOutputHelper outputHelper;
    private readonly SessionInfoService sessionInfoService;
    private readonly ThreadSafeInMemoryRepository<string, SessionDetailRecord> memoryRepository;
    private readonly TypeNameProvider typeNameProvider;

    public SessionInfoServiceShould(ITestOutputHelper outputHelper, KafkaTestsCleanUpFixture _)
    {
        this.outputHelper = outputHelper;
        this.memoryRepository = new ThreadSafeInMemoryRepository<string, SessionDetailRecord>();

        var apiConfigurationProvider =
            new StreamingApiConfigurationProvider(
                new StreamingApiConfiguration(
                    StreamCreationStrategy.PartitionBased,
                    BrokerUrl,
                    [],
                    integrateDataFormatManagement: false,
                    integrateSessionManagement: true));
        var cancellationTokenSourceProvider = new CancellationTokenSourceProvider();
        var logger = Substitute.For<ILogger>();
        var sessionRouteSubscriberFactory = new SessionRouteSubscriberFactory(apiConfigurationProvider, cancellationTokenSourceProvider, logger);
        var packetDtoFromByteFactory = new PacketDtoFromByteFactory(logger);
        var newSessionDtoFromByteFactory = new NewSessionPacketDtoFromByteFactory(logger);
        var endOfSessionDtoFromByteFactory = new EndOfSessionPacketDtoFromByteFactory(logger);
        var sessionInfoDtoFromByteFactory = new SessionInfoPacketDtoFromByteFactory(logger);
        var essentialTopicNameCreator = new EssentialTopicNameCreator();
        this.typeNameProvider = new TypeNameProvider();
        this.sessionInfoService = new SessionInfoService(
            this.memoryRepository,
            sessionRouteSubscriberFactory,
            logger,
            packetDtoFromByteFactory,
            sessionInfoDtoFromByteFactory,
            newSessionDtoFromByteFactory,
            endOfSessionDtoFromByteFactory,
            this.typeNameProvider,
            essentialTopicNameCreator);
        new KafkaClearHelper(BrokerUrl).Clear().Wait();
        Task.Delay(2000).Wait();
    }

    [Fact]
    public void Start_Properly_When_Broker_Is_Available()
    {
        //arrange
        var startingSuccess = false;

        //act
        try
        {
            this.sessionInfoService.Start();
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
    public void Create_Session_Info_Topic_At_Starting_When_Not_Exist()
    {
        //arrange
        var kafkaTopicHelper = new KafkaTopicHelper();
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(BrokerUrl, Constants.SessionInfoTopicName);

        //act

        this.sessionInfoService.Start();
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(BrokerUrl, Constants.SessionInfoTopicName);
        new AutoResetEvent(false).WaitOne(2000);

        //assert
        beforeStartTopicInfos.Any(i => i.TopicName == Constants.SessionInfoTopicName).Should().BeFalse();
        afterStartTopicInfos.Any(i => i.TopicName == Constants.SessionInfoTopicName).Should().BeTrue();
        var sessionInfoTopicInfos = afterStartTopicInfos.Where(i => i.TopicName == Constants.SessionInfoTopicName).ToList();
        sessionInfoTopicInfos.Count.Should().Be(1);
        sessionInfoTopicInfos[0].Partition.Should().Be(0);
        sessionInfoTopicInfos[0].Offset.Should().Be(0);
    }

    [Fact]
    public void Read_All_Exist_Session_Related_Packets_And_Update_The_Repository()
    {
        new AutoResetEvent(false).WaitOne(5000);
        //arrange
        var kafkaPublishHelper = new KafkaPublishHelper(BrokerUrl);
        var kafkaTopicHelper = new KafkaTopicHelper();
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(BrokerUrl, Constants.SessionInfoTopicName);

        const string DataSource = "Session_Info_Test_Datasource";
        MetricProviders.NumberOfSessions.WithLabels(DataSource).Set(0);
        var mapField = new MapField<string, long>
        {
            {
                $"{DataSource}:{0}", 10
            }
        };
        var sessionKey = Guid.NewGuid().ToString();
        var newSessionPacket = new NewSessionPacket
        {
            DataSource = DataSource,
            TopicPartitionOffsets =
            {
                mapField
            }
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
            }
        };

        var addAssociateSessionPacket2 = new SessionInfoPacket
        {
            AssociateSessionKeys =
            {
                associatedId1,
                associatedId2
            }
        };

        var endSessionPacket = new EndOfSessionPacket
        {
            DataSource = DataSource
        };

        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(sessionKey, this.typeNameProvider.NewSessionPacketTypeName, newSessionPacket.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, newSessionInfoPacket.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, updateSessionIdentifierPacket.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, addAssociateSessionPacket1.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(sessionKey, this.typeNameProvider.EndOfSessionPacketTypeName, endSessionPacket.ToByteString()),
            sessionKey);

        //act
        this.sessionInfoService.Start();
        new AutoResetEvent(false).WaitOne(5000);
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(BrokerUrl, Constants.SessionInfoTopicName);
        new AutoResetEvent(false).WaitOne(5000);
        //assert
        beforeStartTopicInfos.Any(i => i.TopicName == Constants.SessionInfoTopicName).Should().BeFalse();
        afterStartTopicInfos.Any(i => i.TopicName == Constants.SessionInfoTopicName).Should().BeTrue();
        var sessions = this.memoryRepository.GetAll(i => i.DataSource == DataSource);
        sessions.Count.Should().Be(1);
        sessions[0].SessionKey.Should().Be(sessionKey);
        sessions[0].Completed.Should().BeTrue();
        sessions[0].StartingOffsetInfo.Count.Should().Be(1);
        sessions[0].StartingOffsetInfo[0].TopicName.Should().Be(DataSource);
        sessions[0].StartingOffsetInfo[0].Partition.Should().Be(0);
        sessions[0].StartingOffsetInfo[0].Offset.Should().Be(10);
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
