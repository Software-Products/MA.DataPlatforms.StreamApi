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

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.SessionManagement;

using NSubstitute;

using Xunit;

namespace MA.Streaming.UnitTests.Services;

public class SessionInfoServiceShould
{
    private const string DataSource = "TestDataSource";
    private const string MainTopicName = "domain.Data.TestDataSource";
    private const string EssentialTopicName = "domain.Essential.TestDataSource";
    private const string SessionKey = "session-key-1";

    private readonly SessionInfoService sut;
    private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository;
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IDataSourcesRepository dataSourcesRepository;

    public SessionInfoServiceShould()
    {
        this.sessionInfoRepository = Substitute.For<IInMemoryRepository<string, SessionDetailRecord>>();
        this.kafkaRouteInfoCreator = Substitute.For<IKafkaRouteInfoCreator>();
        this.dataSourcesRepository = Substitute.For<IDataSourcesRepository>();

        var mainRouteInfo = new KafkaRouteInfo(
            new RouteNameInfo("domain", DataSource, string.Empty, RouteType.Main),
            new KafkaRouteTopicInfo(MainTopicName, 0));
        var essentialRouteInfo = new KafkaRouteInfo(
            new RouteNameInfo("domain", DataSource, string.Empty, RouteType.Essential),
            new KafkaRouteTopicInfo(EssentialTopicName, 0));

        this.kafkaRouteInfoCreator.CreateMainRouteInfo(DataSource).Returns(mainRouteInfo);
        this.kafkaRouteInfoCreator.CreateEssentialRouteInfo(DataSource).Returns(essentialRouteInfo);

        var configProvider = Substitute.For<IStreamingApiConfigurationProvider>();
        configProvider.Provide().Returns(Substitute.For<IStreamingApiConfiguration>());
        configProvider.Provide().InitialisationTimeoutSeconds.Returns(3u);

        var sessionRouteSubscriberFactory = Substitute.For<ISessionRouteSubscriberFactory>();
        var logger = Substitute.For<ILogger>();
        var packetDtoFromByteFactory = Substitute.For<IDtoFromByteFactory<PacketDto>>();
        var sessionInfoDtoFromByteFactory = Substitute.For<IDtoFromByteFactory<SessionInfoPacketDto>>();
        var newSessionDtoFromByteFactory = Substitute.For<IDtoFromByteFactory<NewSessionPacketDto>>();
        var endOfSessionDtoFromByteFactory = Substitute.For<IDtoFromByteFactory<EndOfSessionPacketDto>>();
        var typeNameProvider = Substitute.For<ITypeNameProvider>();

        this.sut = new SessionInfoService(
            configProvider,
            this.sessionInfoRepository,
            sessionRouteSubscriberFactory,
            logger,
            packetDtoFromByteFactory,
            sessionInfoDtoFromByteFactory,
            newSessionDtoFromByteFactory,
            endOfSessionDtoFromByteFactory,
            typeNameProvider,
            this.kafkaRouteInfoCreator,
            this.dataSourcesRepository);
    }

    /// <summary>
    /// Regression test for commit 5d90f35f: MainOffset was always 0 because it was looked up
    /// using the raw DataSource name instead of the actual Kafka topic name produced by
    /// kafkaRouteInfoCreator.CreateMainRouteInfo(). 
    /// </summary>
    [Fact]
    public void ResolveMainOffset_Using_KafkaRouteInfoCreator_TopicName_Not_DataSourceName()
    {
        // Arrange
        const long ExpectedMainOffset = 42L;
        const long ExpectedEssentialOffset = 99L;

        // the TopicOffsetsPartitions list uses the resolved topic names, NOT
        // the raw DataSource string. Before the fix the lookup used DataSource as the key
        // so mainOffset would have been 0 even though the entry is present.
        var topicOffsets = new List<TopicPartitionOffsetDto>
        {
            new(MainTopicName, 0, ExpectedMainOffset),
            new(EssentialTopicName, 0, ExpectedEssentialOffset)
        };

        var newSessionPacket = new NewSessionPacketDto(
            DataSource,
            topicOffsets,
            TimeSpan.Zero,
            null,
            DateTime.UtcNow);

        // capture the session detail record generated
        SessionDetailRecord? capturedRecord = null;
        this.sessionInfoRepository
            .When(r => r.AddOrUpdate(Arg.Any<string>(), Arg.Any<SessionDetailRecord>()))
            .Do(ci => capturedRecord = ci.Arg<SessionDetailRecord>());

        // Act
        var (success, message) = this.sut.AddNewSession(SessionKey, newSessionPacket);

        // Assert
        success.Should().BeTrue(message);
        capturedRecord.Should().NotBeNull();
        capturedRecord!.MainOffset.Should().Be(
            ExpectedMainOffset,
            "MainOffset must be resolved via kafkaRouteInfoCreator.CreateMainRouteInfo(), not by matching the raw DataSource name");
        capturedRecord.EssentialOffset.Should().Be(ExpectedEssentialOffset);
    }
}
