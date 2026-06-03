// <copyright file="SessionDetailsUpdateRequestHandlerShould.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.SessionManagement;
using MA.Streaming.Proto.Core.Abstractions;
using MA.Streaming.Proto.Core.Handlers;

using NSubstitute;

using Xunit;

namespace MA.Streaming.IntegrationTests;

public class SessionDetailsUpdateRequestHandlerShould
{
    private const string SessionKey = "SessionKey";
    private const string DataSource = "DataSource";
    private readonly ITypeNameProvider typeNameProvider;
    private readonly IPacketWriterHelper packetWriterHelper;
    private readonly ILogger logger;
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly ISessionRouteSubscriberFactory sessionRouteSubscriberFactory;
    private readonly IDtoFromByteFactory<PacketDto> packetDtoFromByteFactory;
    private readonly IDtoFromByteFactory<SessionInfoPacketDto> sessionInfoPacketDtoFromByteFactory;
    private readonly IDtoFromByteFactory<NewSessionPacketDto> newSessionPacketDtoFromByteFactory;
    private readonly IDtoFromByteFactory<EndOfSessionPacketDto> endOfSessionPacketDtoFromByteFactory;
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IDataSourcesRepository dataSourcesRepository;

    public SessionDetailsUpdateRequestHandlerShould()
    {
        this.typeNameProvider = Substitute.For<ITypeNameProvider>();
        this.packetWriterHelper = Substitute.For<IPacketWriterHelper>();
        this.logger = Substitute.For<ILogger>();
        this.typeNameProvider.SessionInfoPacketTypeName.Returns("SessionInfo");
        this.streamingApiConfigurationProvider = Substitute.For<IStreamingApiConfigurationProvider>();
        this.sessionRouteSubscriberFactory = Substitute.For<ISessionRouteSubscriberFactory>();
        this.packetDtoFromByteFactory = Substitute.For<IDtoFromByteFactory<PacketDto>>();
        this.sessionInfoPacketDtoFromByteFactory = Substitute.For<IDtoFromByteFactory<SessionInfoPacketDto>>();
        this.newSessionPacketDtoFromByteFactory = Substitute.For<IDtoFromByteFactory<NewSessionPacketDto>>();
        this.endOfSessionPacketDtoFromByteFactory = Substitute.For<IDtoFromByteFactory<EndOfSessionPacketDto>>();
        this.kafkaRouteInfoCreator = Substitute.For<IKafkaRouteInfoCreator>();
        this.dataSourcesRepository = Substitute.For<IDataSourcesRepository>();
    }

    [Fact]
    public void Handle_Session_Details_Update_Request()
    {
        // Arrange
        var sessionInfoRepository = new ThreadSafeInMemoryRepository<string, SessionDetailRecord>();

        var sessionInfoService = new SessionInfoService(
            this.streamingApiConfigurationProvider,
            sessionInfoRepository,
            this.sessionRouteSubscriberFactory,
            this.logger,
            this.packetDtoFromByteFactory,
            this.sessionInfoPacketDtoFromByteFactory,
            this.newSessionPacketDtoFromByteFactory,
            this.endOfSessionPacketDtoFromByteFactory,
            this.typeNameProvider,
            this.kafkaRouteInfoCreator,
            this.dataSourcesRepository);

        var sessionDetailsUpdateRequestHandler = new SessionDetailsUpdateRequestHandler(
            sessionInfoRepository,
            this.logger,
            this.packetWriterHelper,
            this.typeNameProvider,
            sessionInfoService);

        sessionInfoRepository.AddOrUpdate(SessionKey, new SessionDetailRecord(SessionKey, DataSource, [], 0, 0, TimeSpan.Zero, DateTime.UtcNow));

        var sessionDetails = new Dictionary<string, string>
        {
            {
                "Test Key", "Test Value"
            },
            {
                "Test Key 2", "Test Value 2"
            }
        };

        // Act
        var result = sessionDetailsUpdateRequestHandler.UpdateSessionDetails(
            new UpdateSessionDetailsRequest
            {
                Details =
                {
                    sessionDetails
                },
                SessionKey = SessionKey
            });

        // Assert
        result.Success.Should().BeTrue();
        var sessionDetailRecord = sessionInfoRepository.Get(SessionKey);
        sessionDetailRecord.Should().NotBeNull();
        sessionDetailRecord.SessionInfoPacket.Details.Should().BeEquivalentTo(sessionDetails);
    }

    [Fact]
    public async Task Handle_Session_Details_Update_Request_Concurrently()
    {
        // Arrange
        var sessionInfoRepository = new ThreadSafeInMemoryRepository<string, SessionDetailRecord>();

        var sessionInfoService = new SessionInfoService(
            this.streamingApiConfigurationProvider,
            sessionInfoRepository,
            this.sessionRouteSubscriberFactory,
            this.logger,
            this.packetDtoFromByteFactory,
            this.sessionInfoPacketDtoFromByteFactory,
            this.newSessionPacketDtoFromByteFactory,
            this.endOfSessionPacketDtoFromByteFactory,
            this.typeNameProvider,
            this.kafkaRouteInfoCreator,
            this.dataSourcesRepository);

        var sessionDetailsUpdateRequestHandler = new SessionDetailsUpdateRequestHandler(
            sessionInfoRepository,
            this.logger,
            this.packetWriterHelper,
            this.typeNameProvider,
            sessionInfoService);

        sessionInfoRepository.AddOrUpdate(SessionKey, new SessionDetailRecord(SessionKey, DataSource, [], 0, 0, TimeSpan.Zero, DateTime.UtcNow));

        var sessionDetailsUpdateList = new List<Dictionary<string, string>>();
        var sessionDetailsExpectedResult = new Dictionary<string, string>();

        for (var i = 0; i < 10; i++)
        {
            var kvp = new KeyValuePair<string, string>($"Test Key {i}", $"Test Value {i}");
            sessionDetailsUpdateList.Add(
                new Dictionary<string, string>
                {
                    {
                        kvp.Key, kvp.Value
                    }
                });

            sessionDetailsExpectedResult.Add(kvp.Key, kvp.Value);
        }

        var results = new List<UpdateSessionDetailsResponse>();
        var taskList = new List<Task>();

        // Act
        foreach (var sessionDetail in sessionDetailsUpdateList)
        {
            var task = Task.Run(() =>
            {
                var result = sessionDetailsUpdateRequestHandler.UpdateSessionDetails(
                    new UpdateSessionDetailsRequest
                    {
                        Details =
                        {
                            sessionDetail
                        },
                        SessionKey = SessionKey
                    });
                results.Add(result);
            });

            taskList.Add(task);
        }

        await Task.WhenAll(taskList);

        // Assert
        results.All(x => x.Success).Should().BeTrue();
        var sessionDetailRecord = sessionInfoRepository.Get(SessionKey);
        sessionDetailRecord.Should().NotBeNull();
        sessionDetailRecord.SessionInfoPacket.Details.Should().BeEquivalentTo(sessionDetailsExpectedResult);
    }
}
