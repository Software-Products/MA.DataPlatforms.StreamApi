// <copyright file="SessionManagementServiceShould.cs" company="McLaren Applied Ltd.">
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

using Grpc.Core;

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.SessionManagement;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Core.Handlers;
using MA.Streaming.Proto.Core.Model;
using MA.Streaming.Proto.Core.Providers;
using MA.Streaming.Proto.Core.Services;

using NSubstitute;

using Xunit;

namespace MA.Streaming.UnitTests.Services;

public class SessionManagementServiceShould
{
    private const string DataSourceName = "DataSourceName";
    private const string SessionKey = "SessionKey";

    private static readonly Random Rnd = new();
    private readonly SessionManager sessionManager;
    private readonly IRouteInfoProvider routeInfoProvider;
    private readonly IKeyGeneratorService sessionKeyGenerator;
    private readonly IPacketWriterConnectorService writeConnectorService;
    private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoMemoryRepository;
    private readonly ILogger logger;
    private readonly TypeNameProvider typeNameProvider;
    private readonly IStreamsProvider streamsProvider;

    public SessionManagementServiceShould()
    {
        this.sessionKeyGenerator = Substitute.For<IKeyGeneratorService>();
        this.routeInfoProvider = Substitute.For<IRouteInfoProvider>();
        this.streamsProvider = Substitute.For<IStreamsProvider>();
        this.writeConnectorService = Substitute.For<IPacketWriterConnectorService>();
        var sessionInfoService = Substitute.For<ISessionInfoService>();
        this.sessionInfoMemoryRepository = Substitute.For<IInMemoryRepository<string, SessionDetailRecord>>();
        var configProvider = Substitute.For<IStreamingApiConfigurationProvider>();
        configProvider.Provide().IntegrateSessionManagement.Returns(true);
        var cancellationTokenSourceProvider = Substitute.For<ICancellationTokenSourceProvider>();
        cancellationTokenSourceProvider.Provide().Returns(new CancellationTokenSource());
        this.logger = Substitute.For<ILogger>();

        this.sessionKeyGenerator.GenerateStringKey().Returns(_ => SessionKey);

        this.typeNameProvider = new TypeNameProvider();
        this.sessionManager = new SessionManager(
            new SessionCreationRequestHandler(
                this.routeInfoProvider,
                this.typeNameProvider,
                this.sessionKeyGenerator,
                new PacketWriterHelper(this.writeConnectorService)),
            new SessionEndingRequestHandler(
                this.routeInfoProvider,
                this.typeNameProvider,
                this.sessionInfoMemoryRepository,
                new PacketWriterHelper(this.writeConnectorService)),
            new GetSessionInfoRequestHandler(this.sessionInfoMemoryRepository, this.streamsProvider),
            new SessionIdentifierUpdateRequestHandler(
                this.sessionInfoMemoryRepository,
                this.logger,
                new PacketWriterHelper(this.writeConnectorService),
                this.typeNameProvider),
            new AddAssociateSessionRequestHandler(
                this.sessionInfoMemoryRepository,
                this.logger,
                new PacketWriterHelper(this.writeConnectorService),
                this.typeNameProvider),
            new SessionDetailsUpdateRequestHandler(
                this.sessionInfoMemoryRepository,
                this.logger,
                new PacketWriterHelper(this.writeConnectorService),
                this.typeNameProvider),
            this.sessionInfoMemoryRepository,
            configProvider,
            new SessionNotificationManagerService(
                sessionInfoService,
                new NotificationStreamWriterService<GetSessionStartNotificationResponse>(),
                new NotificationStreamWriterService<GetSessionStopNotificationResponse>()));
    }

    [Fact]
    public async Task Call_GenerateKey_GetTopicPartitions_WriteInfoPacket_OnCreateSession()
    {
        // Arrange 
        var routeInfos = CreateRandomRouteInfo(4);
        this.routeInfoProvider.GetRouteInfo(DataSourceName).Returns(routeInfos);
        var request = new CreateSessionRequest
        {
            DataSource = DataSourceName,
            Type = "Session",
            Version = 10
        };
        var context = Substitute.For<ServerCallContext>();

        // Act
        var createSessionResponse = await this.sessionManager.CreateSession(request, context);

        // Assert
        createSessionResponse.SessionKey.Should().Be(SessionKey);
        createSessionResponse.NewSession.DataSource.Should().Be(DataSourceName);
        this.sessionKeyGenerator.Received(1).GenerateStringKey();
        this.routeInfoProvider.Received(1).GetRouteInfo(DataSourceName);
        this.writeConnectorService.Received(2).WriteInfoPacket(Arg.Any<WriteInfoPacketRequestDto>());
        this.writeConnectorService.Received(1).WriteInfoPacket(Arg.Is<WriteInfoPacketRequestDto>(x => x.Message.Type == this.typeNameProvider.NewSessionPacketTypeName));
        this.writeConnectorService.Received(1).WriteInfoPacket(Arg.Is<WriteInfoPacketRequestDto>(x => x.Message.Type == this.typeNameProvider.SessionInfoPacketTypeName));
    }

    [Fact]
    public async Task Call_GetTopicPartitions_WriteInfoPacket_OnEndSession()
    {
        // Arrange 
        var routeInfos = CreateRandomRouteInfo(4);
        this.routeInfoProvider.GetRouteInfo(DataSourceName).Returns(routeInfos);
        var request = new EndSessionRequest
        {
            DataSource = DataSourceName,
            SessionKey = SessionKey
        };
        var context = Substitute.For<ServerCallContext>();
        this.sessionInfoMemoryRepository.Get(SessionKey).Returns(new SessionDetailRecord(SessionKey, DataSourceName, []));

        // Act
        var endSessionResponse = await this.sessionManager.EndSession(request, context);

        // Assert
        endSessionResponse.EndSession.DataSource.Should().Be(DataSourceName);
        endSessionResponse.EndSession.TopicPartitionOffsets.Count.Should().Be(4);
        endSessionResponse.EndSession.TopicPartitionOffsets.Values.Should().BeEquivalentTo(routeInfos.Select(i => i.Offset));
        endSessionResponse.EndSession.TopicPartitionOffsets.Keys.Should().BeEquivalentTo(routeInfos.Select(i => $"{i.Topic}:{i.Partition}"));

        this.writeConnectorService.Received(1).WriteInfoPacket(Arg.Any<WriteInfoPacketRequestDto>());
        this.writeConnectorService.Received(1)
            .WriteInfoPacket(Arg.Is<WriteInfoPacketRequestDto>(x => x.Message.Type == this.typeNameProvider.EndOfSessionPacketTypeName));
    }

    [Fact]
    public async Task Return_Empty_When_There_Is_No_SessionInfo_WriteInfoPacket_OnEndSession()
    {
        // Arrange 
        var routeInfos = CreateRandomRouteInfo(4);
        this.routeInfoProvider.GetRouteInfo(DataSourceName).Returns(routeInfos);
        var request = new EndSessionRequest
        {
            DataSource = DataSourceName,
            SessionKey = SessionKey
        };
        var context = Substitute.For<ServerCallContext>();
        this.sessionInfoMemoryRepository.Get(SessionKey).Returns((SessionDetailRecord?)null);

        // Act
        var endSessionResponse = await this.sessionManager.EndSession(request, context);

        // Assert
        endSessionResponse.EndSession.Should().Be(null);
    }

    [Fact]
    public async Task Return_All_Session_With_Defined_DataSource()
    {
        // Arrange 
        var routeInfos = CreateRandomRouteInfo(4);
        var topicPartitionsOffsetInfo = routeInfos.Select(i => new TopicPartitionOffsetDto(i.Topic, i.Partition, i.Offset)).ToList();
        this.sessionInfoMemoryRepository.GetAll().Returns(
            new List<SessionDetailRecord>
            {
                new(SessionKey, DataSourceName, topicPartitionsOffsetInfo),
                new("testSessionKey", "Unknown", topicPartitionsOffsetInfo),
            });
        var request = new GetCurrentSessionsRequest
        {
            DataSource = DataSourceName
        };
        var context = Substitute.For<ServerCallContext>();

        // Act
        var currentSessionsResponse = await this.sessionManager.GetCurrentSessions(request, context);

        // Assert
        currentSessionsResponse.SessionKeys.Count.Should().Be(1);
        currentSessionsResponse.SessionKeys[0].Should().Be(SessionKey);
    }

    [Fact]
    public async Task Return_SessionInfo_With_Specified_Id_In_The_Request()
    {
        // Arrange 
        var routeInfos = CreateRandomRouteInfo(4);
        var topicPartitionsOffsetInfo = routeInfos.Select(i => new TopicPartitionOffsetDto(i.Topic, i.Partition, i.Offset)).ToList();
        var sessionKey1 = Guid.NewGuid().ToString();
        var sessionKey2 = Guid.NewGuid().ToString();
        var sessionDetailRecord1 = new SessionDetailRecord(sessionKey1, DataSourceName, topicPartitionsOffsetInfo);
        var sessionDetailRecord2 = new SessionDetailRecord(sessionKey2, "DataSource2", topicPartitionsOffsetInfo);
        var sessionDetails1 = new Dictionary<string, string>
        {
            ["DetailName1"] = "DetailValue1"
        };
        IReadOnlyDictionary<string, string> sessionDetails2 = new Dictionary<string, string>
        {
            ["DetailName2"] = "DetailValue2"
        };
        sessionDetailRecord1.SetSessionInfo(new SessionInfoPacketDto("1", 1, "Id1", ["1.1,1.2,1.3"], sessionDetails1));
        sessionDetailRecord2.SetSessionInfo(new SessionInfoPacketDto("2", 2, "Id2", ["2.1,2.2,2.3"], sessionDetails2));
        this.sessionInfoMemoryRepository.Get(Arg.Is(sessionKey1)).Returns(sessionDetailRecord1);
        var request = new GetSessionInfoRequest
        {
            SessionKey = sessionKey1
        };
        var context = Substitute.For<ServerCallContext>();

        // Act
        var getSessionInfoResponse = await this.sessionManager.GetSessionInfo(request, context);

        // Assert
        getSessionInfoResponse.DataSource.Should().Be(DataSourceName);
        getSessionInfoResponse.Type.Should().Be("1");
        getSessionInfoResponse.Version.Should().Be(1);
        getSessionInfoResponse.AssociateSessionKeys.Should().BeEquivalentTo(
            new List<string>
            {
                "1.1,1.2,1.3"
            });
        getSessionInfoResponse.Details.Keys.Should().BeEquivalentTo(sessionDetails1.Keys);
        getSessionInfoResponse.Details.Values.Should().BeEquivalentTo(sessionDetails1.Values);
    }

    [Fact]
    public async Task Log_Error_When_Try_To_Update_Identifier_Of_Not_Exist_Session_Key()
    {
        // Arrange 

        var sessionKey = Guid.NewGuid().ToString();
        this.sessionInfoMemoryRepository.Get(Arg.Is(sessionKey)).Returns((SessionDetailRecord?)null);
        var request = new UpdateSessionIdentifierRequest
        {
            SessionKey = sessionKey
        };
        var context = Substitute.For<ServerCallContext>();

        // Act
        var updateSessionIdentifierResponse = await this.sessionManager.UpdateSessionIdentifier(request, context);

        // Assert
        this.logger.Received(1).Error($"try to update identifier for a session which is not added in the session management service. session key:{sessionKey}");
        updateSessionIdentifierResponse.Success.Should().BeFalse();
    }

    [Fact]
    public async Task Update_Identifier_When_SessionInfo_Exist()
    {
        // Arrange 
        var routeInfos = CreateRandomRouteInfo(4);
        var topicPartitionsOffsetInfo = routeInfos.Select(i => new TopicPartitionOffsetDto(i.Topic, i.Partition, i.Offset)).ToList();
        var sessionKey = Guid.NewGuid().ToString();
        var sessionDetailRecord = new SessionDetailRecord(sessionKey, DataSourceName, topicPartitionsOffsetInfo);
        const string Type = "1";
        const uint Version = 1;
        IReadOnlyList<string> topicPartitionOffset = ["1.1,1.2,1.3"];
        IReadOnlyDictionary<string, string> sessionDetails = new Dictionary<string, string>();
        sessionDetailRecord.SetSessionInfo(new SessionInfoPacketDto(Type, Version, "Id1", topicPartitionOffset, sessionDetails));
        this.sessionInfoMemoryRepository.Get(Arg.Is(sessionKey)).Returns(sessionDetailRecord);
        const string NewIdentifier = "Id2";
        var request = new UpdateSessionIdentifierRequest
        {
            SessionKey = sessionKey,
            Identifier = NewIdentifier
        };
        var context = Substitute.For<ServerCallContext>();

        // Act
        var updateSessionIdentifierResponse = await this.sessionManager.UpdateSessionIdentifier(request, context);

        // Assert
        updateSessionIdentifierResponse.Success.Should().BeTrue();
        this.writeConnectorService.Received(1).WriteInfoPacket(
            Arg.Is<WriteInfoPacketRequestDto>(
                req =>
                    req.Message.Type == this.typeNameProvider.SessionInfoPacketTypeName
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Identifier == NewIdentifier
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Type == Type
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Version == Version
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).AssociateSessionKeys.SequenceEqual(topicPartitionOffset)
            ));
    }

    [Fact]
    public async Task Log_Error_When_Try_To_Update_Details_For_Unknown_Session_Key()
    {
        // Arrange 
        var sessionKey = Guid.NewGuid().ToString();
        this.sessionInfoMemoryRepository.Get(Arg.Is(sessionKey)).Returns((SessionDetailRecord?)null);
        var request = new UpdateSessionDetailsRequest
        {
            SessionKey = sessionKey
        };
        var context = Substitute.For<ServerCallContext>();

        // Act
        var updateSessionDetailsResponse = await this.sessionManager.UpdateSessionDetails(request, context);

        // Assert
        this.logger.Received(1).Error($"Unable to update session details for a session which is not currently registered with the session management service. session key: {sessionKey}");
        updateSessionDetailsResponse.Success.Should().BeFalse();
    }

    [Fact]
    public async Task Update_Details_When_SessionInfo_Exists()
    {
        // Arrange 
        var routeInfos = CreateRandomRouteInfo(4);
        var topicPartitionsOffsetInfo = routeInfos.Select(i => new TopicPartitionOffsetDto(i.Topic, i.Partition, i.Offset)).ToList();
        var sessionKey = Guid.NewGuid().ToString();
        var sessionDetailRecord = new SessionDetailRecord(sessionKey, DataSourceName, topicPartitionsOffsetInfo);
        const string Type = "1";
        const uint Version = 1;
        const string Identifier = "Id";
        IReadOnlyList<string> topicPartitionOffset = ["1.1,1.2,1.3"];
        var sessionDetails = new Dictionary<string, string>();
        sessionDetails["DetailName1"] = "DetailValue1";
        sessionDetailRecord.SetSessionInfo(new SessionInfoPacketDto(Type, Version, Identifier, topicPartitionOffset, sessionDetails));
        this.sessionInfoMemoryRepository.Get(Arg.Is(sessionKey)).Returns(sessionDetailRecord);
        var request = new UpdateSessionDetailsRequest
        {
            SessionKey = sessionKey
        };
        request.Details.Add("DetailName1", "UpdateDetailValue1");
        request.Details.Add("DetailName2", "DetailValue2");

        var context = Substitute.For<ServerCallContext>();

        // Act
        var updateSessionDetailsResponse = await this.sessionManager.UpdateSessionDetails(request, context);

        // Assert
        updateSessionDetailsResponse.Success.Should().BeTrue();
        this.writeConnectorService.Received(1).WriteInfoPacket(
            Arg.Is<WriteInfoPacketRequestDto>(
                req =>
                    req.Message.Type == this.typeNameProvider.SessionInfoPacketTypeName
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Identifier == Identifier
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Type == Type
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Version == Version
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).AssociateSessionKeys.SequenceEqual(topicPartitionOffset)
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Details.Keys.SequenceEqual(request.Details.Keys)
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Details.Values.SequenceEqual(request.Details.Values)
    ));
}

    [Fact]
    public async Task Log_Error_When_Try_To_Add_Associated_Id_To_Not_Exist_Session_Key()
    {
        // Arrange 
        var sessionKey = Guid.NewGuid().ToString();
        this.sessionInfoMemoryRepository.Get(Arg.Is(sessionKey)).Returns((SessionDetailRecord?)null);
        var request = new AddAssociateSessionRequest
        {
            SessionKey = sessionKey
        };
        var context = Substitute.For<ServerCallContext>();

        // Act
        var addAssociateSessionResponse = await this.sessionManager.AddAssociateSession(request, context);

        // Assert
        this.logger.Received(1).Error(
            $"try to add associatedId to an identifier for a session which is not added in the session management service. session key:{request.SessionKey}");
        addAssociateSessionResponse.Success.Should().BeFalse();
    }

    [Fact]
    public async Task Add_Associate_Id_When_SessionInfo_Exist()
    {
        // Arrange 
        var routeInfos = CreateRandomRouteInfo(4);
        var topicPartitionsOffsetInfo = routeInfos.Select(i => new TopicPartitionOffsetDto(i.Topic, i.Partition, i.Offset)).ToList();
        var sessionKey = Guid.NewGuid().ToString();
        var sessionDetailRecord = new SessionDetailRecord(sessionKey, DataSourceName, topicPartitionsOffsetInfo);
        const string Type = "1";
        const uint Version = 1;
        const string Identifier = "Id1";
        IReadOnlyList<string> topicPartitionOffset = ["1.1,1.2,1.3"];
        IReadOnlyDictionary<string, string> sessionDetails = new Dictionary<string, string>();
        sessionDetailRecord.SetSessionInfo(new SessionInfoPacketDto(Type, Version, Identifier, topicPartitionOffset, sessionDetails));
        this.sessionInfoMemoryRepository.Get(Arg.Is(sessionKey)).Returns(sessionDetailRecord);
        const string NewAssociatedId = "1.4";
        var request = new AddAssociateSessionRequest
        {
            SessionKey = sessionKey,
            AssociateSessionKey = NewAssociatedId
        };
        var context = Substitute.For<ServerCallContext>();

        // Act
        var addAssociateSessionResponse = await this.sessionManager.AddAssociateSession(request, context);

        // Assert
        addAssociateSessionResponse.Success.Should().BeTrue();
        this.writeConnectorService.Received(1).WriteInfoPacket(
            Arg.Is<WriteInfoPacketRequestDto>(
                req =>
                    req.Message.Type == this.typeNameProvider.SessionInfoPacketTypeName
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Identifier == Identifier
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Type == Type
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).Version == Version
                    && SessionInfoPacket.Parser.ParseFrom(req.Message.Content).AssociateSessionKeys.SequenceEqual(
                        new List<string>(topicPartitionOffset)
                        {
                            NewAssociatedId
                        })
            ));
    }

    private static IReadOnlyList<KafkaRouteInfo> CreateRandomRouteInfo(int numberOfRouteInfo)
    {
        var result = new List<KafkaRouteInfo>();
        for (var i = 0; i < numberOfRouteInfo; i++)
        {
            result.Add(new KafkaRouteInfo(DataSourceName, DataSourceName, i, Rnd.Next(0, 1000), DataSourceName));
        }

        return result;
    }
}
