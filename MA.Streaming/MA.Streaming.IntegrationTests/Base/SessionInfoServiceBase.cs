// <copyright file="SessionInfoServiceBase.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.OpenData;
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core.Factories;
using MA.Streaming.Proto.Core.Providers;

using NSubstitute;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests.Base;

public abstract class SessionInfoServiceBase
{
    protected abstract string BrokerUrl { get; }

    protected abstract string DomainPrefix { get; }

    protected virtual IKafkaSecurityConfigurationProvider CreateSecurityProvider() =>
        new EmptyKafkaSecurityConfigProvider();

    private readonly ITestOutputHelper outputHelper;
    private readonly Lazy<TestContext> context;
    private readonly ILogger logger;

    protected SessionInfoServiceBase(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
        this.context = new Lazy<TestContext>(this.CreateTestContext);
        this.logger = Substitute.For<ILogger>();
    }

    private TestContext CreateTestContext()
    {
        var memoryRepository = new ThreadSafeInMemoryRepository<string, SessionDetailRecord>();

        var domain = DomainCreator.Create(this.DomainPrefix);
        var security = this.CreateSecurityProvider();

        var apiConfigurationProvider = new StreamingApiConfigurationProvider(
            new StreamingApiConfiguration(
                StreamCreationStrategy.PartitionBased,
                this.BrokerUrl,
                [],
                integrateDataFormatManagement: false,
                integrateSessionManagement: true,
                domain: domain));

        var repository = new RouteReadingWritingComponentRepository<KafkaRouteSubscriber>(this.logger);
        var kafkaRouteManager = new KafkaRouteManager(this.logger);
        var routingMappingInfoRepository = new RoutingMappingInfoRepository();
        var domainNameProvider = new DomainNameProvider(domain);
        var domainKafkaRouteMappingInfoCreator = new DomainKafkaRouteMappingInfoCreator(domainNameProvider, apiConfigurationProvider);
        var kafkaRouteInfoCreator = new KafkaRouteInfoCreator(domainKafkaRouteMappingInfoCreator, apiConfigurationProvider, routingMappingInfoRepository);
        var sessionRouteSubscriberFactory = new SessionRouteSubscriberFactory(
            apiConfigurationProvider,
            kafkaRouteManager,
            kafkaRouteInfoCreator,
            this.logger,
            repository,
            new KafkaSecurityConfigurationProviderAdapter(security));
        var packetDtoFromByteFactory = new PacketDtoFromByteFactory(this.logger);
        var newSessionDtoFromByteFactory = new NewSessionPacketDtoFromByteFactory(this.logger);
        var endOfSessionDtoFromByteFactory = new EndOfSessionPacketDtoFromByteFactory(this.logger);
        var sessionInfoDtoFromByteFactory = new SessionInfoPacketDtoFromByteFactory(this.logger);
        var datasourceRepository = Substitute.For<IDataSourcesRepository>();
        var typeNameProvider = new TypeNameProvider();
        var sessionInfoService = new SessionInfoService(
            apiConfigurationProvider,
            memoryRepository,
            sessionRouteSubscriberFactory,
            this.logger,
            packetDtoFromByteFactory,
            sessionInfoDtoFromByteFactory,
            newSessionDtoFromByteFactory,
            endOfSessionDtoFromByteFactory,
            typeNameProvider,
            kafkaRouteInfoCreator,
            datasourceRepository);

        var sessionInfoTopicName = kafkaRouteInfoCreator.CreateSessionInfoRouteInfo().KafkaRouteTopicInfo.Topic;
        var mainTopicName = kafkaRouteInfoCreator.CreateMainRouteInfo("Session_Info_Test_Datasource").KafkaRouteTopicInfo.Topic;
        var essentialTopicName = kafkaRouteInfoCreator.CreateEssentialRouteInfo("Session_Info_Test_Datasource").KafkaRouteTopicInfo.Topic;

        return new TestContext(sessionInfoService, memoryRepository, typeNameProvider, sessionInfoTopicName, mainTopicName, essentialTopicName);
    }

    private sealed record TestContext(
        SessionInfoService SessionInfoService,
        ThreadSafeInMemoryRepository<string, SessionDetailRecord> MemoryRepository,
        TypeNameProvider TypeNameProvider,
        string SessionInfoTopicName,
        string MainTopicName,
        string EssentialTopicName);

    [Fact]
    public void Start_Properly_When_Broker_Is_Available()
    {
        var startingSuccess = false;
        try
        {
            this.context.Value.SessionInfoService.ServiceStarted += (_, _) =>
            {
                startingSuccess = true;
            };
            this.context.Value.SessionInfoService.Start();
        }
        catch (Exception ex)
        {
            this.outputHelper.WriteLine($"exception: {ex}");
        }

        startingSuccess.Should().BeTrue();
    }

    [Fact]
    public void Create_Session_Info_Topic_At_Starting_When_Not_Exist()
    {
        var security = KafkaSecurityConfigurationMapper.Map(this.CreateSecurityProvider().Provide());
        var kafkaTopicHelper = new KafkaTopicHelper(this.logger);
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(this.BrokerUrl, this.context.Value.SessionInfoTopicName, security);
        this.context.Value.SessionInfoService.Start();
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(this.BrokerUrl, this.context.Value.SessionInfoTopicName, security);
        beforeStartTopicInfos.Any(i => i.TopicName == this.context.Value.SessionInfoTopicName).Should().BeFalse();
        afterStartTopicInfos.Any(i => i.TopicName == this.context.Value.SessionInfoTopicName).Should().BeTrue();
        var sessionInfoTopicInfos = afterStartTopicInfos.Where(i => i.TopicName == this.context.Value.SessionInfoTopicName).ToList();
        sessionInfoTopicInfos.Count.Should().Be(1);
        sessionInfoTopicInfos[0].Partition.Should().Be(0);
        sessionInfoTopicInfos[0].Offset.Should().Be(0);
    }

    [Fact]
    public void Read_All_Exist_Session_Related_Packets_And_Update_The_Repository()
    {
        var security = KafkaSecurityConfigurationMapper.Map(this.CreateSecurityProvider().Provide());
        var kafkaPublishHelper = new KafkaPublishHelper(this.BrokerUrl, security);
        var kafkaTopicHelper = new KafkaTopicHelper(this.logger);
        var beforeStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(this.BrokerUrl, this.context.Value.SessionInfoTopicName, security);
        var serviceStarted = false;
        var serviceStopped = false;
        this.context.Value.SessionInfoService.ServiceStarted += (_, _) => serviceStarted = true;
        this.context.Value.SessionInfoService.ServiceStopped += (_, _) => serviceStopped = true;

        const string DataSource = "Session_Info_Test_Datasource";
        MetricProviders.NumberOfSessions.WithLabels(DataSource).Set(0);
        const long ExpectedMainOffset = 55L;
        const long ExpectedEssentialOffset = 77L;
        var mapField = new MapField<string, long>
        {
            {
                $"{DataSource}:{0}", 10
            },
            {
                $"{this.context.Value.MainTopicName}:{0}", ExpectedMainOffset
            },
            {
                $"{this.context.Value.EssentialTopicName}:{0}", ExpectedEssentialOffset
            }
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
        var sessionInfoPacketName = this.context.Value.TypeNameProvider.SessionInfoPacketTypeName;
        var newSessionInfoPacket = new SessionInfoPacket
        {
            Type = sessionInfoPacketName,
            DataSource = DataSource,
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
            DataSource = DataSource
        };

        kafkaPublishHelper.PublishData(
            this.context.Value.SessionInfoTopicName,
            GetPacketBytes(sessionKey, this.context.Value.TypeNameProvider.NewSessionPacketTypeName, newSessionPacket.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.context.Value.SessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, newSessionInfoPacket.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.context.Value.SessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, updateSessionIdentifierPacket.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.context.Value.SessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, addAssociateSessionPacket1.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.context.Value.SessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.context.Value.SessionInfoTopicName,
            GetPacketBytes(sessionKey, sessionInfoPacketName, addAssociateSessionPacket2.ToByteString()),
            sessionKey);
        kafkaPublishHelper.PublishData(
            this.context.Value.SessionInfoTopicName,
            GetPacketBytes(sessionKey, this.context.Value.TypeNameProvider.EndOfSessionPacketTypeName, endSessionPacket.ToByteString()),
            sessionKey);

        this.context.Value.SessionInfoService.Start();
        var afterStartTopicInfos = kafkaTopicHelper.GetInfoByTopicContains(this.BrokerUrl, this.context.Value.SessionInfoTopicName, security);

        serviceStarted.Should().BeTrue();
        serviceStopped.Should().BeFalse();
        beforeStartTopicInfos.Any(i => i.TopicName == this.context.Value.SessionInfoTopicName).Should().BeFalse();
        afterStartTopicInfos.Any(i => i.TopicName == this.context.Value.SessionInfoTopicName).Should().BeTrue();

        var sessions = this.context.Value.MemoryRepository.GetAll(i => i.DataSource == DataSource);
        sessions.Count.Should().Be(1);
        sessions[0].SessionKey.Should().Be(sessionKey);
        sessions[0].Completed.Should().BeTrue();
        sessions[0].StartingOffsetInfo.Count.Should().Be(3);
        sessions[0].StartingOffsetInfo[0].TopicName.Should().Be(DataSource);
        sessions[0].StartingOffsetInfo[0].Partition.Should().Be(0);
        sessions[0].StartingOffsetInfo[0].Offset.Should().Be(10);
        sessions[0].MainOffset.Should().Be(ExpectedMainOffset, "MainOffset must be resolved via the main topic name");
        sessions[0].EssentialOffset.Should().Be(ExpectedEssentialOffset, "EssentialOffset must be resolved via the essential topic name");
        sessions[0].SessionInfoPacket.Identifier.Should().Be("new_identifier");
        sessions[0].SessionInfoPacket.AssociatedKeys.Should().BeEquivalentTo(
            new List<string>
            {
                associatedId1,
                associatedId2
            });
        sessions[0].SessionInfoPacket.Type.Should().Be(sessionInfoPacketName);
        sessions[0].SessionInfoPacket.Version.Should().Be(1);
        MetricProviders.NumberOfSessions.WithLabels(DataSource).Value.Should().Be(1);

        this.context.Value.SessionInfoService.Stop();
        serviceStopped.Should().BeTrue();
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
