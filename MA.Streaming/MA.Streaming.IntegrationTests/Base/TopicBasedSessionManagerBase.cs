// <copyright file="TopicBasedSessionManagerBase.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.Core.Providers;
using MA.Streaming.Proto.ServerComponent;

using Xunit;

namespace MA.Streaming.IntegrationTests.Base;

public abstract class TopicBasedSessionManagerBase
{
    protected abstract string BrokerUrl { get; }

    protected abstract string DataSource { get; }

    protected abstract string Domain { get; }

    protected virtual string Stream1 => "Stream1";

    protected virtual string Stream2 => "Stream2";

    private const string PreExistSessionKey = "0f8fad5b-d9cb-469f-a165-70867728950e";

    protected virtual IKafkaSecurityConfigurationProvider CreateSecurityProvider() =>
        new EmptyKafkaSecurityConfigProvider();

    private sealed record TestContext(
        SessionManagementService.SessionManagementServiceClient SessionManagementClient,
        string StreamTopic1,
        string StreamTopic2,
        string EssentialTopic,
        string MainStreamTopic);

    private readonly Lazy<TestContext> context;

    protected TopicBasedSessionManagerBase()
    {
        this.context = new Lazy<TestContext>(this.CreateTestContext);
    }

    private TestContext CreateTestContext()
    {
        var security = this.CreateSecurityProvider();
        var kafkaSecurity = KafkaSecurityConfigurationMapper.Map(security.Provide());

        var apiConfigurationProvider = new StreamingApiConfigurationProvider(
            new StreamingApiConfiguration(
                StreamCreationStrategy.TopicBased,
                this.BrokerUrl,
                [],
                integrateDataFormatManagement: false,
                integrateSessionManagement: true,
                domain: this.Domain,
                streamApiPort: ApiPortCreator.CreateNew()));

        var routingMappingInfoRepository = new RoutingMappingInfoRepository();
        var domainNameProvider = new DomainNameProvider(this.Domain);
        var domainKafkaRouteMappingInfoCreator = new DomainKafkaRouteMappingInfoCreator(domainNameProvider, apiConfigurationProvider);
        var kafkaRouteInfoCreator = new KafkaRouteInfoCreator(domainKafkaRouteMappingInfoCreator, apiConfigurationProvider, routingMappingInfoRepository);
        var sessionInfoTopic = kafkaRouteInfoCreator.CreateSessionInfoRouteInfo().KafkaRouteTopicInfo.Topic;

        var streamTopic1 = kafkaRouteInfoCreator.CreateStreamRouteInfo(this.DataSource, this.Stream1).KafkaRouteTopicInfo.Topic;
        var streamTopic2 = kafkaRouteInfoCreator.CreateStreamRouteInfo(this.DataSource, this.Stream2).KafkaRouteTopicInfo.Topic;
        var essentialTopic = kafkaRouteInfoCreator.CreateEssentialRouteInfo(this.DataSource).KafkaRouteTopicInfo.Topic;
        var mainStreamTopic = kafkaRouteInfoCreator.CreateMainRouteInfo(this.DataSource).KafkaRouteTopicInfo.Topic;

        new KafkaTopicCreatorHelper(this.BrokerUrl, kafkaSecurity).Create(new KafkaTopicMetaData(sessionInfoTopic, 1));
        new KafkaTopicCreatorHelper(this.BrokerUrl, kafkaSecurity).Create(new KafkaTopicMetaData(streamTopic1, 1));
        new KafkaTopicCreatorHelper(this.BrokerUrl, kafkaSecurity).Create(new KafkaTopicMetaData(streamTopic2, 1));

        var typeNameProvider = new TypeNameProvider();
        var kafkaPublishHelper = new KafkaPublishHelper(this.BrokerUrl, kafkaSecurity);

        var mapField = new MapField<string, long>
        {
            {
                $"{mainStreamTopic}:{0}", 10
            },
            {
                $"{streamTopic1}:{0}", 5
            },
            {
                $"{streamTopic2}:{0}", 20
            }
        };
        var newSessionPacket = new NewSessionPacket
        {
            DataSource = this.DataSource,
            TopicPartitionOffsets =
            {
                mapField
            },
            UtcOffset = Duration.FromTimeSpan(TimeSpan.FromHours(-2))
        };
        var sessionInfoPacketName = typeNameProvider.SessionInfoPacketTypeName;
        var newSessionInfoPacket = new SessionInfoPacket
        {
            Type = sessionInfoPacketName,
            DataSource = this.DataSource,
            Version = 1
        };
        var updateSessionIdentifierPacket = new SessionInfoPacket
        {
            Identifier = "new_identifier"
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
            DataSource = this.DataSource
        };

        kafkaPublishHelper.PublishData(
            sessionInfoTopic,
            GetPacketBytes(PreExistSessionKey, typeNameProvider.NewSessionPacketTypeName, newSessionPacket.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopic,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, newSessionInfoPacket.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopic,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, updateSessionIdentifierPacket.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopic,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, addAssociateSessionPacket1.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopic,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopic,
            GetPacketBytes(PreExistSessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            PreExistSessionKey);
        kafkaPublishHelper.PublishData(
            sessionInfoTopic,
            GetPacketBytes(PreExistSessionKey, typeNameProvider.EndOfSessionPacketTypeName, endSessionPacket.ToByteString()),
            PreExistSessionKey);
        new AutoResetEvent(false).WaitOne(5000);

        var streamApiClient = StreamingApiClientFactory.Create(
            apiConfigurationProvider.Provide(),
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(security),
            new LoggingDirectoryProvider(""),
            kafkaSecurityConfigProvider: security);
        streamApiClient.Initialise();

        return new TestContext(streamApiClient.GetSessionManagementClient(), streamTopic1, streamTopic2, essentialTopic, mainStreamTopic);
    }

    [Fact]
    public void Do_The_Session_Manipulation_Actions_Properly()
    {
        var cancellationToken = new CancellationTokenSource().Token;
        var ctx = this.context.Value;
        var response = this.context.Value.SessionManagementClient.GetCurrentSessions(
            new GetCurrentSessionsRequest
            {
                DataSource = this.DataSource
            });
        response.SessionKeys.Count.Should().Be(1);
        response.SessionKeys[0].Should().Be(PreExistSessionKey);

        var sessionStartNotification = this.context.Value.SessionManagementClient.GetSessionStartNotification(
            new GetSessionStartNotificationRequest
            {
                DataSource = this.DataSource
            });
        var startAutoResetEvent = new AutoResetEvent(false);
        var startNotificationReceived = false;
        var notificationSessionKey = "";
        _ = Task.Run(
            async () =>
            {
                while (await sessionStartNotification.ResponseStream.MoveNext(cancellationToken))
                {
                    var n = sessionStartNotification.ResponseStream.Current;
                    notificationSessionKey = n.SessionKey;
                    startNotificationReceived = true;
                    startAutoResetEvent.Set();
                }
            },
            cancellationToken);

        var creationTime = Timestamp.FromDateTime(new DateTime(new DateOnly(2024, 6, 1), new TimeOnly(0, 0, 0), DateTimeKind.Utc));
        var createSessionResponse = this.context.Value.SessionManagementClient.CreateSession(
            new CreateSessionRequest
            {
                DataSource = this.DataSource,
                Type = "Session",
                Version = 20,
                UtcOffset = Duration.FromTimeSpan(TimeSpan.FromHours(-2)),
                CreationTime = creationTime
            });
        createSessionResponse.NewSession.DataSource.Should().Be(this.DataSource);
        createSessionResponse.NewSession.TopicPartitionOffsets.Count.Should().Be(4);
        IEnumerable<string> expectation = [$"{ctx.MainStreamTopic}:{0}", $"{ctx.EssentialTopic}:{0}", $"{ctx.StreamTopic1}:{0}", $"{ctx.StreamTopic2}:{0}"];
        createSessionResponse.NewSession.TopicPartitionOffsets.Keys.OrderBy(i => i).Should().BeEquivalentTo(expectation);
        createSessionResponse.NewSession.TopicPartitionOffsets.Values.Should().BeEquivalentTo([0, 0, 0, 0]);
        var createdSessionKey = createSessionResponse.SessionKey;
        startAutoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        startNotificationReceived.Should().BeTrue();
        createSessionResponse.SessionKey.Should().Be(notificationSessionKey);
        Guid.Parse(createdSessionKey).Should().NotBeEmpty();

        var getCurrentSessionsResponse = this.context.Value.SessionManagementClient.GetCurrentSessions(
            new GetCurrentSessionsRequest
            {
                DataSource = this.DataSource
            });
        getCurrentSessionsResponse.SessionKeys.Count.Should().Be(2);
        getCurrentSessionsResponse.SessionKeys.OrderBy(i => i).Should().BeEquivalentTo(
            new List<string>
            {
                PreExistSessionKey,
                createdSessionKey
            }.OrderBy(i => i));

        var getReq2 = new GetSessionInfoRequest
        {
            SessionKey = createdSessionKey
        };
        var getSessionInfoResponse1 = this.context.Value.SessionManagementClient.GetSessionInfo(
            new GetSessionInfoRequest
            {
                SessionKey = PreExistSessionKey
            });
        var getSessionInfoResponse2 = this.context.Value.SessionManagementClient.GetSessionInfo(getReq2);
        getSessionInfoResponse1.IsComplete.Should().BeTrue();
        getSessionInfoResponse2.Type.Should().Be("Session");
        getSessionInfoResponse2.Version.Should().Be(20);
        getSessionInfoResponse2.AssociateSessionKeys.Count.Should().Be(0);
        getSessionInfoResponse2.IsComplete.Should().BeFalse();
        getSessionInfoResponse2.DataSource.Should().Be(this.DataSource);
        getSessionInfoResponse2.Identifier.Should().Be("");
        getSessionInfoResponse2.UtcOffset.Should().Be(Duration.FromTimeSpan(TimeSpan.FromHours(-2)));
        getSessionInfoResponse2.CreationTime.Should().Be(creationTime);
        getSessionInfoResponse2.TerminationTime.Should().BeNull();

        var updateSessionIdentifierResponse = this.context.Value.SessionManagementClient.UpdateSessionIdentifier(
            new UpdateSessionIdentifierRequest
            {
                SessionKey = createdSessionKey,
                Identifier = "test_identifier"
            });
        getSessionInfoResponse2 = this.context.Value.SessionManagementClient.GetSessionInfo(getReq2);
        updateSessionIdentifierResponse.Success.Should().BeTrue();
        getSessionInfoResponse2.Identifier.Should().Be("test_identifier");

        var associateSessionKey = Guid.NewGuid().ToString();
        var addAssociateSessionResponse = this.context.Value.SessionManagementClient.AddAssociateSession(
            new AddAssociateSessionRequest
            {
                SessionKey = createdSessionKey,
                AssociateSessionKey = associateSessionKey
            });
        getSessionInfoResponse2 = this.context.Value.SessionManagementClient.GetSessionInfo(getReq2);
        addAssociateSessionResponse.Success.Should().BeTrue();
        getSessionInfoResponse2.AssociateSessionKeys.Count.Should().Be(1);
        getSessionInfoResponse2.AssociateSessionKeys[0].Should().Be(associateSessionKey);

        var terminationTime = Timestamp.FromDateTime(new DateTime(new DateOnly(2024, 6, 1), new TimeOnly(1, 0, 0), DateTimeKind.Utc));
        var sessionStopNotification = this.context.Value.SessionManagementClient.GetSessionStopNotification(
            new GetSessionStopNotificationRequest
            {
                DataSource = this.DataSource
            });
        var endAutoResetEvent = new AutoResetEvent(false);
        var endNotificationReceived = false;
        _ = Task.Run(
            async () =>
            {
                while (await sessionStopNotification.ResponseStream.MoveNext(cancellationToken))
                {
                    var n = sessionStopNotification.ResponseStream.Current;
                    n.SessionKey.Should().Be(createdSessionKey);
                    endNotificationReceived = true;
                    endAutoResetEvent.Set();
                }
            },
            cancellationToken);
        var endSessionResponse = this.context.Value.SessionManagementClient.EndSession(
            new EndSessionRequest
            {
                SessionKey = createdSessionKey,
                TerminationTime = terminationTime
            });
        getSessionInfoResponse2 = this.context.Value.SessionManagementClient.GetSessionInfo(getReq2);
        endAutoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        endNotificationReceived.Should().BeTrue();
        endSessionResponse.EndSession.DataSource.Should().Be(this.DataSource);
        endSessionResponse.EndSession.TopicPartitionOffsets.Count.Should().Be(4);
        getSessionInfoResponse2.IsComplete.Should().BeTrue();
        getSessionInfoResponse2.AssociateSessionKeys[0].Should().Be(associateSessionKey);
        getSessionInfoResponse2.Identifier.Should().Be("test_identifier");
        getSessionInfoResponse2.TerminationTime.Should().Be(terminationTime);
        getSessionInfoResponse2.CreationTime.Should().Be(creationTime);
    }

    [Fact]
    public void Create_And_End_Session_Without_Setting_Creation_And_Termination_Time()
    {
        var createSessionResponse = this.context.Value.SessionManagementClient.CreateSession(
            new CreateSessionRequest
            {
                DataSource = this.DataSource,
                Type = "Session",
                Version = 20,
                UtcOffset = Duration.FromTimeSpan(TimeSpan.FromHours(-2))
            });
        createSessionResponse.Success.Should().BeTrue();
        createSessionResponse.NewSession.DataSource.Should().Be(this.DataSource);
        var createdSessionKey = createSessionResponse.SessionKey;
        Guid.Parse(createdSessionKey).Should().NotBeEmpty();

        var getSessionInfoResponse = this.context.Value.SessionManagementClient.GetSessionInfo(
            new GetSessionInfoRequest
            {
                SessionKey = createdSessionKey
            });
        getSessionInfoResponse.CreationTime.ToDateTime().Date.Should().Be(DateTime.UtcNow.Date);
        getSessionInfoResponse.TerminationTime.Should().BeNull();

        var endSessionResponse = this.context.Value.SessionManagementClient.EndSession(
            new EndSessionRequest
            {
                SessionKey = createdSessionKey
            });
        getSessionInfoResponse = this.context.Value.SessionManagementClient.GetSessionInfo(
            new GetSessionInfoRequest
            {
                SessionKey = createdSessionKey
            });
        endSessionResponse.Success.Should().BeTrue();
        getSessionInfoResponse.TerminationTime.ToDateTime().Date.Should().Be(DateTime.UtcNow.Date);
        getSessionInfoResponse.CreationTime.ToDateTime().Date.Should().Be(DateTime.UtcNow.Date);
    }

    private static byte[] GetPacketBytes(string sessionKey, string packetType, ByteString content) =>
        new Packet
        {
            Content = content,
            IsEssential = false,
            SessionKey = sessionKey,
            Type = packetType
        }.ToByteArray();
}
