// <copyright file="PartitionBasedSessionManagerShould.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatform.Secu4.KafkaMetadataComponent;
using MA.DataPlatform.Secu4.Routing.Contracts;
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

using Xunit;

namespace MA.Streaming.IntegrationTests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class PartitionBasedSessionManagerShould : IClassFixture<KafkaTestsCleanUpFixture>
{
    private const string BrokerUrl = "localhost:9097";
    private const string DataSource = "Session_Manager_Test_DataSource";
    private const string Stream1 = "stream1";
    internal const string Stream2 = "stream2";
    private const string PreExistSessionKey = "0f8fad5b-d9cb-469f-a165-70867728950e";
    private readonly SessionManagementService.SessionManagementServiceClient sessionManagementClient;
    private readonly TypeNameProvider typeNameProvider = new();

    public PartitionBasedSessionManagerShould(KafkaTestsCleanUpFixture _)
    {
        var topicsInfo = new KafkaTopicHelper().GetInfoByTopicContains(BrokerUrl, "");
        if (topicsInfo.All(i => i.TopicName != Constants.SessionInfoTopicName))
        {
            new KafkaTopicCreatorHelper(BrokerUrl).Create(new KafkaTopicMetaData(Constants.SessionInfoTopicName, 1));
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
            GetPacketBytes(PreExistSessionKey, this.typeNameProvider.NewSessionPacketTypeName, newSessionPacket.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, newSessionInfoPacket.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, updateSessionIdentifierPacket.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, addAssociateSessionPacket1.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            Constants.SessionInfoTopicName,
            GetPacketBytes(PreExistSessionKey, this.typeNameProvider.EndOfSessionPacketTypeName, endSessionPacket.ToByteString()),
            PreExistSessionKey);
        Task.Delay(5000).Wait();
        var apiConfigurationProvider =
            new StreamingApiConfigurationProvider(
                new StreamingApiConfiguration(
                    StreamCreationStrategy.PartitionBased,
                    BrokerUrl,
                    [new PartitionMapping(Stream1, 1), new PartitionMapping(Stream2, 2)],
                    integrateDataFormatManagement: false,
                    integrateSessionManagement: true));

        StreamingApiClient.Initialise(apiConfigurationProvider.Provide(), new CancellationTokenSourceProvider(), new KafkaBrokerAvailabilityChecker(), new LoggingDirectoryProvider(""));
        this.sessionManagementClient = StreamingApiClient.GetSessionManagementClient();
        new AutoResetEvent(false).WaitOne(5000);
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
        var createSessionRequest = new CreateSessionRequest
        {
            DataSource = DataSource,
            Type = "Session",
            Version = 20,
        };

        //act
        var createSessionResponse = this.sessionManagementClient.CreateSession(
            createSessionRequest);
        new AutoResetEvent(false).WaitOne(1000);

        //assert
        createSessionResponse.NewSession.DataSource.Should().Be(DataSource);
        createSessionResponse.NewSession.TopicPartitionOffsets.Count.Should().Be(4);
        IEnumerable<string> expectation = [$"{DataSource}:{0}", $"{DataSource}_essential:{0}", $"{DataSource}:{1}", $"{DataSource}:{2}"];
        createSessionResponse.NewSession.TopicPartitionOffsets.Keys.Should().BeEquivalentTo(expectation);
        createSessionResponse.NewSession.TopicPartitionOffsets.Values.Should().BeEquivalentTo([0, 0, 0, 0]);
        var createdSessionKey = createSessionResponse.SessionKey;
        startNotificationReceived.Should().BeTrue();
        createSessionResponse.SessionKey.Should().Be(notificationSessionKey);
        Guid.Parse(createdSessionKey).Should().NotBeEmpty();

        //////////////////////////////////////Get Current Sessions///////////////////////////////////////////////////////////////////
        new AutoResetEvent(false).WaitOne(1000);
        //arrange
        getCurrentSessionsRequest = new GetCurrentSessionsRequest
        {
            DataSource = DataSource
        };

        //act
        var getCurrentSessionsResponse = this.sessionManagementClient.GetCurrentSessions(
            getCurrentSessionsRequest);

        //assert
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

        getSessionInfoResponse2.Type.Should().Be("Session");
        getSessionInfoResponse2.Version.Should().Be(20);
        getSessionInfoResponse2.AssociateSessionKeys.Count.Should().Be(0);
        getSessionInfoResponse2.IsComplete.Should().BeFalse();
        getSessionInfoResponse2.DataSource.Should().Be(DataSource);
        getSessionInfoResponse2.Identifier.Should().Be("");

        ////////////////////////////////////////Updating The Session Identifiers///////////////////////////////////////////////////////////////////////

        //arrange
        var updateSessionIdentifierRequest = new UpdateSessionIdentifierRequest
        {
            SessionKey = createdSessionKey,
            Identifier = "test_identifier"
        };

        //act
        var updateSessionIdentifierResponse = this.sessionManagementClient.UpdateSessionIdentifier(updateSessionIdentifierRequest);
        new AutoResetEvent(false).WaitOne(1000);
        getSessionInfoResponse2 = this.sessionManagementClient.GetSessionInfo(getSessionInfoRequest2);
        //assert
        updateSessionIdentifierResponse.Success.Should().BeTrue();
        getSessionInfoResponse2.Type.Should().Be("Session");
        getSessionInfoResponse2.Version.Should().Be(20);
        getSessionInfoResponse2.AssociateSessionKeys.Count.Should().Be(0);
        getSessionInfoResponse2.IsComplete.Should().BeFalse();
        getSessionInfoResponse2.DataSource.Should().Be(DataSource);
        getSessionInfoResponse2.Identifier.Should().Be("test_identifier");

        ////////////////////////////////////////Adding Associate SessionKeys///////////////////////////////////////////////////////////////////////

        //arrange
        var associateSessionKey = Guid.NewGuid().ToString();
        var addAssociateSessionRequest = new AddAssociateSessionRequest
        {
            SessionKey = createdSessionKey,
            AssociateSessionKey = associateSessionKey,
        };

        //act
        var addAssociateSessionResponse = this.sessionManagementClient.AddAssociateSession(addAssociateSessionRequest);
        new AutoResetEvent(false).WaitOne(1000);
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

        ////////////////////////////////////////Ending Of the Session///////////////////////////////////////////////////////////////////////

        //arrange
        var endSessionRequest = new EndSessionRequest
        {
            SessionKey = createdSessionKey
        };
        var sessionStopNotification = this.sessionManagementClient.GetSessionStopNotification(
            new GetSessionStopNotificationRequest
            {
                DataSource = DataSource
            });
        var endNotificationReceived = false;
        _ = Task.Run(
            async () =>
            {
                while (await sessionStopNotification.ResponseStream.MoveNext(cancellationToken))
                {
                    var notification = sessionStopNotification.ResponseStream.Current;
                    notification.SessionKey.Should().Be(createdSessionKey);
                    endNotificationReceived = true;
                }
            },
            cancellationToken);
        //act
        var endSessionResponse = this.sessionManagementClient.EndSession(endSessionRequest);
        new AutoResetEvent(false).WaitOne(1000);
        getSessionInfoResponse2 = this.sessionManagementClient.GetSessionInfo(getSessionInfoRequest2);

        //assert
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
