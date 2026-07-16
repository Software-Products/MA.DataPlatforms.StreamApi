// <copyright file="DataFormatManagerBase.cs" company="Motion Applied Ltd.">
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

using MA.Common;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

using Xunit;

namespace MA.Streaming.IntegrationTests.Base;

public abstract class DataFormatManagerBase
{
    protected abstract string BrokerUrl { get; }
    protected abstract string DomainPrefix { get; }
    protected abstract string DataSourcePrefix { get; }
    protected virtual string Stream1 => "Stream1";
    protected virtual string Stream2 => "Stream2";
    private const string PreExistEventIdentifier = "event1";

    protected virtual IKafkaSecurityConfigurationProvider CreateSecurityProvider() =>
        new EmptyKafkaSecurityConfigProvider();

    private readonly Lazy<TestContext> context;

    protected DataFormatManagerBase()
    {
        this.context = new Lazy<TestContext>(this.CreateTestContext);
    }

    private TestContext CreateTestContext()
    {
        var security = this.CreateSecurityProvider();
        var kafkaSecurity = KafkaSecurityConfigurationMapper.Map(security.Provide());
        var keyGenerator = new KeyGeneratorService(new LoggingDirectoryProvider(""));
        var kafkaPublishHelper = new KafkaPublishHelper(this.BrokerUrl, kafkaSecurity);

        var domain1 = DomainCreator.Create(this.DomainPrefix);
        var dataSource = DataSourceCreator.Create(this.DataSourcePrefix);

        var preExistParamUlongIdentifier = keyGenerator.GenerateUlongKey();
        var preExistParameterIdentifiersList = new List<string> { "Param1", "Param2", "Param3" };
        var parameterDataFormatDefinitionPacket = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Parameter,
            ParameterIdentifiers = new ParameterList { ParameterIdentifiers = { preExistParameterIdentifiersList } },
            Identifier = preExistParamUlongIdentifier
        };

        var preExistEventUlongIdentifier = keyGenerator.GenerateUlongKey();
        var eventDataFormatDefinitionPacket = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Event,
            EventIdentifier = PreExistEventIdentifier,
            Identifier = preExistEventUlongIdentifier
        };

        var apiConfigurationProvider = new StreamingApiConfigurationProvider(
            new StreamingApiConfiguration(
                StreamCreationStrategy.PartitionBased,
                this.BrokerUrl,
                [new PartitionMapping(this.Stream1, 1), new PartitionMapping(this.Stream2, 2)],
                integrateDataFormatManagement: true,
                integrateSessionManagement: false,
                domain: domain1,
                streamApiPort: ApiPortCreator.CreateNew()));

        var routingMappingInfoRepository = new RoutingMappingInfoRepository();
        var domainNameProvider = new DomainNameProvider(domain1);
        var domainKafkaRouteMappingInfoCreator = new DomainKafkaRouteMappingInfoCreator(domainNameProvider, apiConfigurationProvider);
        var kafkaRouteInfoCreator = new KafkaRouteInfoCreator(domainKafkaRouteMappingInfoCreator, apiConfigurationProvider, routingMappingInfoRepository);
        var essentialTopic = kafkaRouteInfoCreator.CreateEssentialRouteInfo(dataSource).KafkaRouteTopicInfo.Topic;

        kafkaPublishHelper.PublishData(essentialTopic, GetPacketBytes(nameof(DataFormatDefinitionPacket), parameterDataFormatDefinitionPacket.ToByteString()));
        kafkaPublishHelper.PublishData(essentialTopic, GetPacketBytes(nameof(DataFormatDefinitionPacket), eventDataFormatDefinitionPacket.ToByteString()));
        Thread.Sleep(5000);

        var streamingApiClient = StreamingApiClientFactory.Create(
            apiConfigurationProvider.Provide(),
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(security),
            new LoggingDirectoryProvider(""),
            new ConsoleLogger(),
            kafkaSecurityConfigProvider: security);
        streamingApiClient.Initialise();
        var dataFormatManagerServiceClient = streamingApiClient.GetDataFormatManagerClient();

        return new TestContext(dataSource, dataFormatManagerServiceClient, preExistEventUlongIdentifier, preExistParameterIdentifiersList, preExistParamUlongIdentifier);
    }

    [Fact]
    public void Initialise_The_Data_Format_Management_Repository_With_Exist_Data()
    {
        var eventDataFormatIdResponse = this.context.Value.DataFormatManagerServiceClient.GetEventDataFormatId(new GetEventDataFormatIdRequest { DataSource = this.context.Value.DataSource, Event = PreExistEventIdentifier });
        eventDataFormatIdResponse.DataFormatIdentifier.Should().Be(this.context.Value.PreExistEventUlongIdentifier);

        var getParameterDataFormatIdResponse = this.context.Value.DataFormatManagerServiceClient.GetParameterDataFormatId(new GetParameterDataFormatIdRequest { DataSource = this.context.Value.DataSource, Parameters = { this.context.Value.PreExistParameterIdentifiersList } });
        getParameterDataFormatIdResponse.DataFormatIdentifier.Should().Be(this.context.Value.PreExistParamUlongIdentifier);

        this.context.Value.DataFormatManagerServiceClient.GetEvent(new GetEventRequest { DataSource = this.context.Value.DataSource, DataFormatIdentifier = this.context.Value.PreExistEventUlongIdentifier }).Event.Should().Be(PreExistEventIdentifier);
        this.context.Value.DataFormatManagerServiceClient.GetParametersList(new GetParametersListRequest { DataSource = this.context.Value.DataSource, DataFormatIdentifier = this.context.Value.PreExistParamUlongIdentifier }).Parameters.Should().BeEquivalentTo(this.context.Value.PreExistParameterIdentifiersList);
    }

    [Fact]
    public void Do_The_Data_Format_Management_Manipulation_Actions_Properly()
    {
        const string NewEvent = "NewEvent";
        var eventDataFormatIdResponse = this.context.Value.DataFormatManagerServiceClient.GetEventDataFormatId(new GetEventDataFormatIdRequest { DataSource = this.context.Value.DataSource, Event = NewEvent });
        eventDataFormatIdResponse.DataFormatIdentifier.Should().BeGreaterThan(0);
        this.context.Value.DataFormatManagerServiceClient.GetEvent(new GetEventRequest { DataSource = this.context.Value.DataSource, DataFormatIdentifier = eventDataFormatIdResponse.DataFormatIdentifier }).Event.Should().Be(NewEvent);

        const string NewParameter1 = "NewParameter1"; const string NewParameter2 = "NewParameter2";
        var getParameterDataFormatIdResponse = this.context.Value.DataFormatManagerServiceClient.GetParameterDataFormatId(new GetParameterDataFormatIdRequest { DataSource = this.context.Value.DataSource, Parameters = { NewParameter1, NewParameter2 } });
        getParameterDataFormatIdResponse.DataFormatIdentifier.Should().BeGreaterThan(0);
        this.context.Value.DataFormatManagerServiceClient.GetParametersList(new GetParametersListRequest { DataSource = this.context.Value.DataSource, DataFormatIdentifier = getParameterDataFormatIdResponse.DataFormatIdentifier }).Parameters.Should().BeEquivalentTo(new List<string> { NewParameter1, NewParameter2 });
    }

    private static byte[] GetPacketBytes(string packetType, ByteString content) =>
        new Packet { Content = content, IsEssential = false, SessionKey = "", Type = packetType.Replace(nameof(Packet), "") }.ToByteArray();

    private sealed record TestContext(
        string DataSource,
        DataFormatManagerService.DataFormatManagerServiceClient DataFormatManagerServiceClient,
        ulong PreExistEventUlongIdentifier,
        List<string> PreExistParameterIdentifiersList,
        ulong PreExistParamUlongIdentifier);
}
