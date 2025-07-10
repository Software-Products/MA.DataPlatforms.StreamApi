// <copyright file="DataFormatManagerShould.cs" company="McLaren Applied Ltd.">
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

using MA.Common;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.Core.Routing.EssentialsRouting;
using MA.Streaming.IntegrationTests.Base;
using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

using Xunit;

namespace MA.Streaming.IntegrationTests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class DataFormatManagerShould : IClassFixture<KafkaTestsCleanUpFixture>
{
    private const string BrokerUrl = "localhost:9097";
    private const string DataSource = "DataFormat_Manager_Test_DataSource";
    private const string Stream1 = "stream1";
    internal const string Stream2 = "stream2";
    private const string PreExistEventIdentifier = "event1";
    private readonly DataFormatManagerService.DataFormatManagerServiceClient dataFormatManagerServiceClient;
    private readonly ulong preExistEventUlongIdentifier;
    private readonly List<string> preExistParameterIdentifiersList;
    private readonly ulong preExistParamUlongIdentifier;

    public DataFormatManagerShould(KafkaTestsCleanUpFixture _)
    {
        var keyGenerator = new KeyGeneratorService(new LoggingDirectoryProvider(""));
        var essentialTopicNameCreator = new EssentialTopicNameCreator();
        var kafkaPublishHelper = new KafkaPublishHelper(BrokerUrl);
        var essentialTopic = essentialTopicNameCreator.Create(DataSource);
        new KafkaClearHelper(BrokerUrl).Clear().Wait();
        StreamingApiClient.Shutdown();
        
        this.preExistParamUlongIdentifier = keyGenerator.GenerateUlongKey();
        this.preExistParameterIdentifiersList =
        [
            "Param1",
            "Param2",
            "Param3"
        ];
        var parameterDataFormatDefinitionPacket = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Parameter,
            ParameterIdentifiers = new ParameterList
            {
                ParameterIdentifiers =
                {
                    this.preExistParameterIdentifiersList
                }
            },
            Identifier = this.preExistParamUlongIdentifier
        };

        this.preExistEventUlongIdentifier = keyGenerator.GenerateUlongKey();

        var eventDataFormatDefinitionPacket = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Event,
            EventIdentifier = PreExistEventIdentifier,
            Identifier = this.preExistEventUlongIdentifier
        };

        kafkaPublishHelper.PublishData(
            essentialTopic,
            GetPacketBytes(nameof(DataFormatDefinitionPacket), parameterDataFormatDefinitionPacket.ToByteString()));
        kafkaPublishHelper.PublishData(
            essentialTopic,
            GetPacketBytes(nameof(DataFormatDefinitionPacket), eventDataFormatDefinitionPacket.ToByteString()));
        Task.Delay(5000).Wait();
        var apiConfigurationProvider =
            new StreamingApiConfigurationProvider(
                new StreamingApiConfiguration(
                    StreamCreationStrategy.PartitionBased,
                    BrokerUrl,
                    [new PartitionMapping(Stream1, 1), new PartitionMapping(Stream2, 2)],
                    integrateDataFormatManagement: true,
                    integrateSessionManagement: false));

        StreamingApiClient.Initialise(
            apiConfigurationProvider.Provide(),
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(),
            new LoggingDirectoryProvider(""));

        this.dataFormatManagerServiceClient = StreamingApiClient.GetDataFormatManagerClient();
    }

    [Fact]
    public void Initialise_The_Data_Format_Management_Repository_With_Exist_Data()
    {
        //arrange
        var eventDataFormatIdRequest = new GetEventDataFormatIdRequest
        {
            DataSource = DataSource,
            Event = PreExistEventIdentifier
        };

        //act
        var eventDataFormatIdResponse = this.dataFormatManagerServiceClient.GetEventDataFormatId(eventDataFormatIdRequest);

        //assert
        var eventDataFormat = eventDataFormatIdResponse.DataFormatIdentifier;
        eventDataFormat.Should().Be(this.preExistEventUlongIdentifier);

        //arrange
        var parameterDataFormatIdRequest = new GetParameterDataFormatIdRequest
        {
            DataSource = DataSource,
            Parameters =
            {
                this.preExistParameterIdentifiersList
            }
        };

        //act
        var getParameterDataFormatIdResponse = this.dataFormatManagerServiceClient.GetParameterDataFormatId(parameterDataFormatIdRequest);

        //assert
        var parameterDataFormat = getParameterDataFormatIdResponse.DataFormatIdentifier;
        parameterDataFormat.Should().Be(this.preExistParamUlongIdentifier);

        //arrange
        var getEventRequest = new GetEventRequest
        {
            DataSource = DataSource,
            DataFormatIdentifier = eventDataFormat
        };

        //act
        var getEventResponse = this.dataFormatManagerServiceClient.GetEvent(getEventRequest);

        //assert
        getEventResponse.Event.Should().Be(PreExistEventIdentifier);

        //arrange
        var getParametersListRequest = new GetParametersListRequest
        {
            DataSource = DataSource,
            DataFormatIdentifier = parameterDataFormat
        };

        //act
        var getParametersListResponse = this.dataFormatManagerServiceClient.GetParametersList(getParametersListRequest);

        //assert
        getParametersListResponse.Parameters.Should().BeEquivalentTo(this.preExistParameterIdentifiersList);
    }

    [Fact]
    public void Do_The_Data_Format_Management_Manipulation_Actions_Properly()
    {
        /////////////////////////////////////Get Id For New Event////////////////////////////////////////////////////////////////////////
        //arrange
        const string NewEvent = "NewEvent";
        var eventDataFormatIdRequest = new GetEventDataFormatIdRequest
        {
            DataSource = DataSource,
            Event = NewEvent
        };

        //act
        var eventDataFormatIdResponse = this.dataFormatManagerServiceClient.GetEventDataFormatId(eventDataFormatIdRequest);

        //assert
        var eventDataFormatIdentifier = eventDataFormatIdResponse.DataFormatIdentifier;

        eventDataFormatIdentifier.Should().BeGreaterThan(0);

        //////////////////////////////////////Get New Event Data///////////////////////////////////////////////////////////////////
        //arrange
        var getEventRequest = new GetEventRequest
        {
            DataSource = DataSource,
            DataFormatIdentifier = eventDataFormatIdentifier
        };

        //act
        var getEventResponse = this.dataFormatManagerServiceClient.GetEvent(getEventRequest);

        //assert
        getEventResponse.Event.Should().Be(NewEvent);

        ///////////////////////////////////////Get Id For New Parameter///////////////////////////////////////////////////////////////////////
        //arrange
        const string NewParameter1 = "NewParameter1";
        const string NewParameter2 = "NewParameter2";
        var parameterDataFormatIdRequest = new GetParameterDataFormatIdRequest
        {
            DataSource = DataSource,
            Parameters =
            {
                NewParameter1,
                NewParameter2
            }
        };

        //act

        var getParameterDataFormatIdResponse = this.dataFormatManagerServiceClient.GetParameterDataFormatId(parameterDataFormatIdRequest);

        //assert
        var dataFormatIdentifier = getParameterDataFormatIdResponse.DataFormatIdentifier;

        dataFormatIdentifier.Should().BeGreaterThan(0);
        //////////////////////////////////////Get New Parameter Data///////////////////////////////////////////////////////////////////
        //arrange
        var getParametersListRequest = new GetParametersListRequest
        {
            DataSource = DataSource,
            DataFormatIdentifier = dataFormatIdentifier
        };

        //act

        var getParametersListResponse = this.dataFormatManagerServiceClient.GetParametersList(getParametersListRequest);

        //assert
        getParametersListResponse.Parameters.Should().BeEquivalentTo(
            new List<string>
            {
                NewParameter1,
                NewParameter2
            });
    }

    private static byte[] GetPacketBytes(string packetType, ByteString content)
    {
        return new Packet
        {
            Content = content,
            IsEssential = false,
            SessionKey = "",
            Type = packetType.Replace(nameof(Packet), "")
        }.ToByteArray();
    }
}
