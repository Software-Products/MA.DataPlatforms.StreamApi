// <copyright file="StreamsProviderTests.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core;
using MA.Streaming.Core.Routing;

using NSubstitute;

using Xunit;

namespace MA.Streaming.UnitTests;

public class StreamsProviderTests
{
    private readonly IKafkaTopicHelper kafkaTopicHelper;
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IStreamingApiConfigurationProvider configurationProvider;
    private readonly IStreamingApiConfiguration configuration;

    public StreamsProviderTests()
    {
        this.kafkaTopicHelper = Substitute.For<IKafkaTopicHelper>();
        this.kafkaRouteInfoCreator = Substitute.For<IKafkaRouteInfoCreator>();
        this.configurationProvider = Substitute.For<IStreamingApiConfigurationProvider>();
        this.configuration = Substitute.For<IStreamingApiConfiguration>();

        this.configurationProvider.Provide().Returns(this.configuration);
    }

    [Fact]
    public void Provide_TopicBasedStrategy_ReturnsExpectedStreams()
    {
        // Arrange
        const string DataSource = "Default";
        this.configuration.StreamCreationStrategy.Returns(StreamCreationStrategy.TopicBased);
        const string Domain = "DefaultDomain";
        this.configuration.Domain.Returns(Domain);
        this.configuration.BrokerUrl.Returns("localhost:9092");

        var mockRouteInfo = Substitute.For<KafkaRouteInfo>(
            new RouteNameInfo(Domain, DataSource, ConstantSystemKeyStreamNames.MainStreamName, RouteType.Main),
            new KafkaRouteTopicInfo("DefaultDomain.Data.DefaultDataSource", 0));

        this.kafkaRouteInfoCreator.CreateMainRouteInfo(DataSource).Returns(mockRouteInfo);

        var topicInfos = new List<TopicInfo>
        {
            new("DefaultDomain.Data.DefaultDataSource", 0, 0),
            new("DefaultDomain.Data.DefaultDataSource.Arbitrator", 0, 100),
            new("DefaultDomain.Data.DefaultDataSource.BIOS", 0, 200),
            new("DefaultDomain.Data.DefaultDataSource.BrakeControl", 0, 300),
            new("DefaultDomain.Data.DefaultDataSource.CANP", 0, 400),
            new("DefaultDomain.Data.DefaultDataSource.Chassis", 0, 500),
            new("DefaultDomain.Data.DefaultDataSource.Controller", 0, 600),
            new("DefaultDomain.Data.DefaultDataSource.Coordinator", 0, 700),
            new("DefaultDomain.Data.DefaultDataSource.Dash", 0, 800),
            new("DefaultDomain.Data.DefaultDataSource.FIA", 0, 900),
            new("DefaultDomain.Data.DefaultDataSource.MRLTrackProcessing", 0, 1000),
            new("DefaultDomain.Data.DefaultDataSource.MathMRL", 0, 1100),
            new("DefaultDomain.Data.DefaultDataSource.PUMaths", 0, 1200),
            new("DefaultDomain.Data.DefaultDataSource.Reliability", 0, 1300),
            new("DefaultDomain.Data.DefaultDataSource.TAG320BIOS", 0, 1400),
            new("DefaultDomain.Essentials.DefaultDataSource", 0, 1500),
            new("DefaultDomain.System.DeadLetter", 0, 1600),
            new("DefaultDomain.System.SessionInfo", 0, 1700),
            new("DefaultDomain.System.Status", 0, 1800)
        };

        this.kafkaTopicHelper
            .GetInfoByTopicPrefix("localhost:9092", mockRouteInfo.KafkaRouteTopicInfo.Topic)
            .Returns(topicInfos);

        var streamsProvider = new StreamsProvider(
            this.configurationProvider,
            this.kafkaTopicHelper,
            this.kafkaRouteInfoCreator);

        var expectedStreams = new List<string>
        {
            "Arbitrator",
            "BIOS",
            "BrakeControl",
            "CANP",
            "Chassis",
            "Controller",
            "Coordinator",
            "Dash",
            "FIA",
            "MRLTrackProcessing",
            "MathMRL",
            "PUMaths",
            "Reliability",
            "TAG320BIOS"
        };

        // Act
        var result = streamsProvider.Provide(DataSource);

        // Assert
        Assert.Equal(expectedStreams.Count, result.Count);
        Assert.All(expectedStreams, stream => Assert.Contains(stream, result));
    }

    [Fact]
    public void Provide_PartitionBasedStrategy_ReturnsExpectedStreams()
    {
        // Arrange
        const string DataSource = "DefaultDataSource";
        var partitionMappings = new List<PartitionMapping>
        {
            new("Arbitrator", 0),
            new("BIOS", 1),
            new("BrakeControl", 2),
            new("CANP", 3),
            new("Chassis", 4),
            new("Controller", 5),
            new("Coordinator", 6),
            new("Dash", 7),
            new("FIA", 8),
            new("MRLTrackProcessing", 9),
            new("MathMRL", 10),
            new("PUMaths", 11),
            new("Reliability", 12),
            new("TAG320BIOS", 13)
        };

        this.configuration.StreamCreationStrategy.Returns(StreamCreationStrategy.PartitionBased);
        this.configuration.PartitionMappings.Returns(partitionMappings);

        var streamsProvider = new StreamsProvider(
            this.configurationProvider,
            this.kafkaTopicHelper,
            this.kafkaRouteInfoCreator);

        var expectedStreams = new List<string>
        {
            "Arbitrator",
            "BIOS",
            "BrakeControl",
            "CANP",
            "Chassis",
            "Controller",
            "Coordinator",
            "Dash",
            "FIA",
            "MRLTrackProcessing",
            "MathMRL",
            "PUMaths",
            "Reliability",
            "TAG320BIOS"
        };

        // Act
        var result = streamsProvider.Provide(DataSource);

        // Assert
        Assert.Equal(expectedStreams.Count, result.Count);
        Assert.All(expectedStreams, stream => Assert.Contains(stream, result));
    }

    [Fact]
    public void Provide_TopicBasedStrategy_CachesResultsForOneSecond()
    {
        // Arrange
        const string DataSource = "DefaultDataSource";
        const string Domain = "DefaultDomain";
        this.configuration.StreamCreationStrategy.Returns(StreamCreationStrategy.TopicBased);
        this.configuration.BrokerUrl.Returns("localhost:9092");

        var mockRouteInfo = Substitute.For<KafkaRouteInfo>(
            new RouteNameInfo(Domain, DataSource, ConstantSystemKeyStreamNames.MainStreamName, RouteType.Main),
            new KafkaRouteTopicInfo("DefaultDomain.Data.DefaultDataSource", 0));

        this.kafkaRouteInfoCreator.CreateMainRouteInfo(DataSource).Returns(mockRouteInfo);

        var topicInfos = new List<TopicInfo>
        {
            new("DefaultDomain.Data.DefaultDataSource.Stream1", 0, 100)
        };

        this.kafkaTopicHelper
            .GetInfoByTopicPrefix("localhost:9092", DataSource)
            .Returns(topicInfos);

        var streamsProvider = new StreamsProvider(
            this.configurationProvider,
            this.kafkaTopicHelper,
            this.kafkaRouteInfoCreator);

        // Act
        var result1 = streamsProvider.Provide(DataSource);
        var result2 = streamsProvider.Provide(DataSource);

        // Assert
        Assert.Equal(result1, result2);
        this.kafkaTopicHelper.Received(1).GetInfoByTopicPrefix("localhost:9092", mockRouteInfo.KafkaRouteTopicInfo.Topic);
    }

    [Fact]
    public void Provide_PartitionBasedStrategy_WithNullMappings_ReturnsEmptyList()
    {
        // Arrange
        const string DataSource = "Default";
        this.configuration.StreamCreationStrategy.Returns(StreamCreationStrategy.PartitionBased);
        this.configuration.PartitionMappings.Returns((IReadOnlyList<PartitionMapping>?)null);

        var streamsProvider = new StreamsProvider(
            this.configurationProvider,
            this.kafkaTopicHelper,
            this.kafkaRouteInfoCreator);

        // Act
        var result = streamsProvider.Provide(DataSource);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void Provide_TopicBasedStrategy_FiltersTopicsNotMatchingPattern()
    {
        // Arrange
        const string DataSource = "DefaultDataSource";
        const string Domain = "DefaultDomain";

        this.configuration.StreamCreationStrategy.Returns(StreamCreationStrategy.TopicBased);
        this.configuration.BrokerUrl.Returns("localhost:9092");

        var mockRouteInfo = Substitute.For<KafkaRouteInfo>(
            new RouteNameInfo(Domain, DataSource, ConstantSystemKeyStreamNames.MainStreamName, RouteType.Main),
            new KafkaRouteTopicInfo("DefaultDomain.Data.DefaultDataSource", 0));

        this.kafkaRouteInfoCreator.CreateMainRouteInfo(DataSource).Returns(mockRouteInfo);

        var topicInfos = new List<TopicInfo>
        {
            new("DefaultDomain.Data.DefaultDataSource", 0, 0),
            new("DefaultDomain.Data.DefaultDataSource.Stream1", 0, 100),
            new("DefaultDomain.Data.DefaultDataSource.Stream2.Extra", 0, 200), // Shouldn't filter out (multiple dots after dataSource)
            new("DefaultDomain.Essentials.DefaultDataSource", 0, 300), // Should be filtered out (different prefix)
            new("DefaultDomain.Data.DefaultDataSource.Stream3", 0, 400)
        };

        this.kafkaTopicHelper
            .GetInfoByTopicPrefix("localhost:9092", mockRouteInfo.KafkaRouteTopicInfo.Topic)
            .Returns(topicInfos);

        var streamsProvider = new StreamsProvider(
            this.configurationProvider,
            this.kafkaTopicHelper,
            this.kafkaRouteInfoCreator);

        // Act
        var result = streamsProvider.Provide(DataSource);

        // Assert
        Assert.Equal(3, result.Count);
        Assert.Contains("Stream1", result);
        Assert.Contains("Stream3", result);
        Assert.Contains("Stream2.Extra", result);
    }

    [Fact]
    public void Provide_UnknownStrategy_ReturnsEmptyList()
    {
        // Arrange
        const string DataSource = "Default";
        this.configuration.StreamCreationStrategy.Returns((StreamCreationStrategy)999);

        var streamsProvider = new StreamsProvider(
            this.configurationProvider,
            this.kafkaTopicHelper,
            this.kafkaRouteInfoCreator);

        // Act
        var result = streamsProvider.Provide(DataSource);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void Provide_TopicBasedStrategy_MultiplePartitionsForSameTopic_ReturnsUniqueStreams()
    {
        // Arrange
        const string DataSource = "DefaultDataSource";
        const string Domain = "DefaultDomain";
        this.configuration.StreamCreationStrategy.Returns(StreamCreationStrategy.TopicBased);
        this.configuration.BrokerUrl.Returns("localhost:9092");

        var mockRouteInfo = Substitute.For<KafkaRouteInfo>(
            new RouteNameInfo(Domain, DataSource, ConstantSystemKeyStreamNames.MainStreamName, RouteType.Main),
            new KafkaRouteTopicInfo("DefaultDomain.Data.DefaultDataSource", 0));

        this.kafkaRouteInfoCreator.CreateMainRouteInfo(DataSource).Returns(mockRouteInfo);

        var topicInfos = new List<TopicInfo>
        {
            new("DefaultDomain.Data.DefaultDataSource.Arbitrator", 0, 100),
            new("DefaultDomain.Data.DefaultDataSource.Arbitrator", 1, 200),
            new("DefaultDomain.Data.DefaultDataSource.BIOS", 0, 300),
            new("DefaultDomain.Data.DefaultDataSource.BIOS", 1, 400)
        };

        this.kafkaTopicHelper
            .GetInfoByTopicPrefix("localhost:9092", mockRouteInfo.KafkaRouteTopicInfo.Topic)
            .Returns(topicInfos);

        var streamsProvider = new StreamsProvider(
            this.configurationProvider,
            this.kafkaTopicHelper,
            this.kafkaRouteInfoCreator);

        // Act
        var result = streamsProvider.Provide(DataSource);

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Contains("Arbitrator", result);
        Assert.Contains("BIOS", result);
    }
}
