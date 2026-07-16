// <copyright file="PartitionBasedSessionManagerShould.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.IntegrationTests.Base;
using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.Core.Providers;
using MA.Streaming.Proto.ServerComponent;

using NSubstitute;

using Xunit;

namespace MA.Streaming.IntegrationTests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class PartitionBasedSessionManagerShould
{
    private const string BrokerUrl = "localhost:9098";
    private const string DataSource = "PBSMSDataSource";
    private const string Domain = "PBSMS";
    private const string Stream1 = "Stream1";
    internal const string Stream2 = "Stream2";
    private const string PreExistSessionKey = "0f8fad5b-d9cb-469f-a165-70867728950e";
    private readonly SessionManagementService.SessionManagementServiceClient sessionManagementClient;
    private readonly string streamTopic1;
    private readonly string streamTopic2;
    private readonly string essentialTopic;
    private readonly string mainStreamTopic;
    private readonly TypeNameProvider typeNameProvider = new();

    public PartitionBasedSessionManagerShould()
    {
        var apiConfigurationProvider =
            new StreamingApiConfigurationProvider(
                new StreamingApiConfiguration(
                    StreamCreationStrategy.PartitionBased,
                    BrokerUrl,
                    [new PartitionMapping(Stream1, 1), new PartitionMapping(Stream2, 2)],
                    integrateDataFormatManagement: false,
                    integrateSessionManagement: true,
                    domain: Domain,
                    streamApiPort: ApiPortCreator.CreateNew()));

        var routingMappingInfoRepository = new RoutingMappingInfoRepository();
        var logger = Substitute.For<ILogger>();
        var domainNameProvider = new DomainNameProvider(Domain);
        var domainKafkaRouteMappingInfoCreator = new DomainKafkaRouteMappingInfoCreator(domainNameProvider, apiConfigurationProvider);
        var kafkaRouteInfoCreator = new KafkaRouteInfoCreator(domainKafkaRouteMappingInfoCreator, apiConfigurationProvider, routingMappingInfoRepository);
        var sessionInfoTopicName = kafkaRouteInfoCreator.CreateSessionInfoRouteInfo().KafkaRouteTopicInfo.Topic;
        this.streamTopic1 = kafkaRouteInfoCreator.CreateStreamRouteInfo(DataSource, Stream1).KafkaRouteTopicInfo.Topic;
        this.streamTopic2 = kafkaRouteInfoCreator.CreateStreamRouteInfo(DataSource, Stream2).KafkaRouteTopicInfo.Topic;
        this.essentialTopic = kafkaRouteInfoCreator.CreateEssentialRouteInfo(DataSource).KafkaRouteTopicInfo.Topic;
        this.mainStreamTopic = kafkaRouteInfoCreator.CreateMainRouteInfo(DataSource).KafkaRouteTopicInfo.Topic;

        var topicsInfo = new KafkaTopicHelper(logger).GetInfoByTopicContains(BrokerUrl, "");
        if (topicsInfo.All(i => i.TopicName != sessionInfoTopicName))
        {
            new KafkaTopicCreatorHelper(BrokerUrl).Create(new KafkaTopicMetaData(sessionInfoTopicName, 1));
        }

        var kafkaPublishHelper = new KafkaPublishHelper(BrokerUrl);
        var mapField = new MapField<string, long>
        {
            {
                $"{DataSource}:{0}", 10
            }
        };
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
            Version = Version
        };

        const string NewIdentifier = "new_identifier";
        var updateSessionIdentifierPacket = new SessionInfoPacket
        {
            Identifier = NewIdentifier
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
            sessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, this.typeNameProvider.NewSessionPacketTypeName, newSessionPacket.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, newSessionInfoPacket.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, updateSessionIdentifierPacket.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, addAssociateSessionPacket1.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, this.typeNameProvider.EndOfSessionPacketTypeName, endSessionPacket.ToByteString()),
            PreExistSessionKey);
        Task.Delay(5000).Wait();

        var streamingApiClient = StreamingApiClientFactory.Create(
            apiConfigurationProvider.Provide(),
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(),
            new LoggingDirectoryProvider(""));
        streamingApiClient.Initialise();
        Task.Delay(5000).Wait();
        this.sessionManagementClient = streamingApiClient.GetSessionManagementClient();
    }

    [Fact]
    public void Do_The_Session_Manipulation_Actions_Properly()
    {
        var cancellationToken = new CancellationTokenSource().Token;
        /////////////////////////////////////Get Initial Session Info////////////////////////////////////////////////////////////////////
        //arrange
        var getCurrentSessionsRequest = new GetCurrentSessionsRequest
        {
            DataSource = DataSource
        };

        //act

        var response = this.sessionManagementClient.GetCurrentSessions(
            getCurrentSessionsRequest);

        //assert
        response.SessionKeys.Count.Should().Be(1);
        response.SessionKeys[0].Should().Be(PreExistSessionKey);

        /////////////////////////////////////Creation of a Session////////////////////////////////////////////////////////////////////////
        //arrange
        var sessionStartNotification = this.sessionManagementClient.GetSessionStartNotification(
            new GetSessionStartNotificationRequest
            {
                DataSource = DataSource
            });

        var startNotificationReceived = false;
        var notificationSessionKey = "";
        _ = Task.Run(
            async () =>
            {
                while (await sessionStartNotification.ResponseStream.MoveNext(cancellationToken))
                {
                    var notification = sessionStartNotification.ResponseStream.Current;
                    notificationSessionKey = notification.SessionKey;
                    startNotificationReceived = true;
                }
            },
            cancellationToken);
        var creationTime = Timestamp.FromDateTime(new DateTime(new DateOnly(2024, 6, 1), new TimeOnly(0, 0, 0), DateTimeKind.Utc));
        var createSessionRequest = new CreateSessionRequest
        {
            DataSource = DataSource,
            Type = "Session",
            Version = 20,
            UtcOffset = Duration.FromTimeSpan(TimeSpan.FromHours(-2)),
            CreationTime = creationTime
        };

        //act
        var createSessionResponse = this.sessionManagementClient.CreateSession(createSessionRequest);
        //assert
        createSessionResponse.Success.Should().BeTrue();
        createSessionResponse.NewSession.DataSource.Should().Be(DataSource);
        createSessionResponse.NewSession.TopicPartitionOffsets.Count.Should().Be(4);
        IEnumerable<string> expectation = [$"{this.mainStreamTopic}:{0}", $"{this.essentialTopic}:{0}", $"{this.streamTopic1}:{1}", $"{this.streamTopic2}:{2}"];
        createSessionResponse.NewSession.TopicPartitionOffsets.Keys.Should().BeEquivalentTo(expectation);
        createSessionResponse.NewSession.TopicPartitionOffsets.Values.Should().BeEquivalentTo([0, 0, 0, 0]);
        var createdSessionKey = createSessionResponse.SessionKey;
        startNotificationReceived.Should().BeTrue();
        createSessionResponse.SessionKey.Should().Be(notificationSessionKey);
        Guid.Parse(createdSessionKey).Should().NotBeEmpty();

        //////////////////////////////////////Get Current Sessions///////////////////////////////////////////////////////////////////
        //arrange
        getCurrentSessionsRequest = new GetCurrentSessionsRequest
        {
            DataSource = DataSource
        };

        //act
        var getCurrentSessionsResponse = this.sessionManagementClient.GetCurrentSessions(
            getCurrentSessionsRequest);

        //assert
        getCurrentSessionsResponse.Success.Should().BeTrue();
        getCurrentSessionsResponse.SessionKeys.Count.Should().Be(2);
        getCurrentSessionsResponse.SessionKeys.OrderBy(i => i).Should().BeEquivalentTo(
            new List<string>
            {
                PreExistSessionKey,
                createdSessionKey
            }.OrderBy(i => i));

        ////////////////////////////////////////Get The SessionInfo///////////////////////////////////////////////////////////////////////

        //arrange
        var getSessionInfoRequest2 = new GetSessionInfoRequest
        {
            SessionKey = createdSessionKey
        };

        //act
        var getSessionInfoResponse2 = this.sessionManagementClient.GetSessionInfo(getSessionInfoRequest2);

        //assert
        getSessionInfoResponse2.Success.Should().BeTrue();
        getSessionInfoResponse2.Type.Should().Be("Session");
        getSessionInfoResponse2.Version.Should().Be(20);
        getSessionInfoResponse2.AssociateSessionKeys.Count.Should().Be(0);
        getSessionInfoResponse2.IsComplete.Should().BeFalse();
        getSessionInfoResponse2.DataSource.Should().Be(DataSource);
        getSessionInfoResponse2.Identifier.Should().Be("");
        getSessionInfoResponse2.UtcOffset.Should().Be(Duration.FromTimeSpan(TimeSpan.FromHours(-2)));
        getSessionInfoResponse2.CreationTime.Should().Be(creationTime);
        getSessionInfoResponse2.TerminationTime.Should().BeNull();

        ////////////////////////////////////////Updating The Session Identifiers///////////////////////////////////////////////////////////////////////

        //arrange
        var updateSessionIdentifierRequest = new UpdateSessionIdentifierRequest
        {
            SessionKey = createdSessionKey,
            Identifier = "test_identifier"
        };

        //act
        var updateSessionIdentifierResponse = this.sessionManagementClient.UpdateSessionIdentifier(updateSessionIdentifierRequest);
        getSessionInfoResponse2 = this.sessionManagementClient.GetSessionInfo(getSessionInfoRequest2);
        //assert
        updateSessionIdentifierResponse.Success.Should().BeTrue();
        getSessionInfoResponse2.Type.Should().Be("Session");
        getSessionInfoResponse2.Version.Should().Be(20);
        getSessionInfoResponse2.AssociateSessionKeys.Count.Should().Be(0);
        getSessionInfoResponse2.IsComplete.Should().BeFalse();
        getSessionInfoResponse2.DataSource.Should().Be(DataSource);
        getSessionInfoResponse2.Identifier.Should().Be("test_identifier");
        getSessionInfoResponse2.UtcOffset.Should().Be(Duration.FromTimeSpan(TimeSpan.FromHours(-2)));

        ////////////////////////////////////////Adding Associate SessionKeys///////////////////////////////////////////////////////////////////////

        //arrange
        var associateSessionKey = Guid.NewGuid().ToString();
        var addAssociateSessionRequest = new AddAssociateSessionRequest
        {
            SessionKey = createdSessionKey,
            AssociateSessionKey = associateSessionKey
        };

        //act
        var addAssociateSessionResponse = this.sessionManagementClient.AddAssociateSession(addAssociateSessionRequest);
        getSessionInfoResponse2 = this.sessionManagementClient.GetSessionInfo(getSessionInfoRequest2);

        //assert
        addAssociateSessionResponse.Success.Should().BeTrue();
        getSessionInfoResponse2.Type.Should().Be("Session");
        getSessionInfoResponse2.Version.Should().Be(20);
        getSessionInfoResponse2.AssociateSessionKeys.Count.Should().Be(1);
        getSessionInfoResponse2.AssociateSessionKeys[0].Should().Be(associateSessionKey);
        getSessionInfoResponse2.IsComplete.Should().BeFalse();
        getSessionInfoResponse2.DataSource.Should().Be(DataSource);
        getSessionInfoResponse2.Identifier.Should().Be("test_identifier");
        getSessionInfoResponse2.UtcOffset.Should().Be(Duration.FromTimeSpan(TimeSpan.FromHours(-2)));

        ////////////////////////////////////////Ending Of the Session///////////////////////////////////////////////////////////////////////

        //arrange
        var terminationTime = Timestamp.FromDateTime(new DateTime(new DateOnly(2024, 6, 1), new TimeOnly(1, 0, 0), DateTimeKind.Utc));
        var endSessionRequest = new EndSessionRequest
        {
            SessionKey = createdSessionKey,
            TerminationTime = terminationTime
        };
        var sessionStopNotification = this.sessionManagementClient.GetSessionStopNotification(
            new GetSessionStopNotificationRequest
            {
                DataSource = DataSource
            });
        var endAutoResetEvent = new AutoResetEvent(false);
        var endNotificationReceived = false;
        _ = Task.Run(
            async () =>
            {
                while (await sessionStopNotification.ResponseStream.MoveNext(cancellationToken))
                {
                    var notification = sessionStopNotification.ResponseStream.Current;
                    notification.SessionKey.Should().Be(createdSessionKey);
                    endNotificationReceived = true;
                    endAutoResetEvent.Set();
                }
            },
            cancellationToken);
        //act
        var endSessionResponse = this.sessionManagementClient.EndSession(endSessionRequest);
        getSessionInfoResponse2 = this.sessionManagementClient.GetSessionInfo(getSessionInfoRequest2);
        endAutoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        //assert
        endSessionResponse.Success.Should().BeTrue();
        endNotificationReceived.Should().BeTrue();
        endSessionResponse.EndSession.DataSource.Should().Be(DataSource);
        endSessionResponse.EndSession.TopicPartitionOffsets.Count.Should().Be(4);
        getSessionInfoResponse2.Type.Should().Be("Session");
        getSessionInfoResponse2.Version.Should().Be(20);
        getSessionInfoResponse2.AssociateSessionKeys.Count.Should().Be(1);
        getSessionInfoResponse2.AssociateSessionKeys[0].Should().Be(associateSessionKey);
        getSessionInfoResponse2.IsComplete.Should().BeTrue();
        getSessionInfoResponse2.DataSource.Should().Be(DataSource);
        getSessionInfoResponse2.Identifier.Should().Be("test_identifier");
        getSessionInfoResponse2.UtcOffset.Should().Be(Duration.FromTimeSpan(TimeSpan.FromHours(-2)));
        getSessionInfoResponse2.TerminationTime.Should().Be(terminationTime);
        getSessionInfoResponse2.CreationTime.Should().Be(creationTime);
    }

    [Fact]
    public void Create_And_End_Session_Without_Setting_Creation_And_Termination_Time()
    {
        ////////////////////////////////////Creation of a Session////////////////////////////////////////////////////////////////////////
        // Arrange
        var createSessionRequest = new CreateSessionRequest
        {
            DataSource = DataSource,
            Type = "Session",
            Version = 20,
            UtcOffset = Duration.FromTimeSpan(TimeSpan.FromHours(-2))
        };

        //act
        var createSessionResponse = this.sessionManagementClient.CreateSession(createSessionRequest);
        //assert
        createSessionResponse.Success.Should().BeTrue();
        createSessionResponse.NewSession.DataSource.Should().Be(DataSource);
        createSessionResponse.NewSession.TopicPartitionOffsets.Count.Should().Be(4);
        IEnumerable<string> expectation = [$"{this.mainStreamTopic}:{0}", $"{this.essentialTopic}:{0}", $"{this.streamTopic1}:{1}", $"{this.streamTopic2}:{2}"];
        createSessionResponse.NewSession.TopicPartitionOffsets.Keys.Should().BeEquivalentTo(expectation);
        createSessionResponse.NewSession.TopicPartitionOffsets.Values.Should().BeEquivalentTo([0, 0, 0, 0]);
        var createdSessionKey = createSessionResponse.SessionKey;
        Guid.Parse(createdSessionKey).Should().NotBeEmpty();

        ////////////////////////////////////////Get Session Info///////////////////////////////////////////////////////////////////////
        // Arrange
        var getSessionInfoRequest = new GetSessionInfoRequest
        {
            SessionKey = createdSessionKey
        };

        // Act
        var getSessionInfoResponse = this.sessionManagementClient.GetSessionInfo(getSessionInfoRequest);

        // Assert
        getSessionInfoResponse.CreationTime.ToDateTime().Date.Should().Be(DateTime.UtcNow.Date);
        getSessionInfoResponse.TerminationTime.Should().BeNull();

        ////////////////////////////////////////Ending Of the Session///////////////////////////////////////////////////////////////////////
        // Arrange
        var endSessionRequest = new EndSessionRequest
        {
            SessionKey = createdSessionKey
        };

        // Act
        var endSessionResponse = this.sessionManagementClient.EndSession(endSessionRequest); 
        getSessionInfoResponse = this.sessionManagementClient.GetSessionInfo(getSessionInfoRequest);

        // Assert
        endSessionResponse.Success.Should().BeTrue();
        getSessionInfoResponse.TerminationTime.ToDateTime().Date.Should().Be(DateTime.UtcNow.Date);
        getSessionInfoResponse.CreationTime.ToDateTime().Date.Should().Be(DateTime.UtcNow.Date);
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
