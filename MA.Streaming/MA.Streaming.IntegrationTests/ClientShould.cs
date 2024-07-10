// <copyright file="ClientShould.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.IntegrationTests.Base;
using MA.Streaming.Proto.Client.Local;

using NSubstitute;

using Xunit;

namespace MA.Streaming.IntegrationTests;

public class ClientShould : IClassFixture<StreamApiTestsCleanUpFixture>
{
    private readonly IStreamingApiConfiguration streamingApiConfiguration;
    private readonly IKafkaBrokerAvailabilityChecker kafkaChecker;

    public ClientShould(StreamApiTestsCleanUpFixture _)
    {
        // Ensure each test run starts from as "clean" a state as possible
        StreamingApiClient.Shutdown();
        this.streamingApiConfiguration = Substitute.For<IStreamingApiConfiguration>();
        this.streamingApiConfiguration.BrokerUrl.Returns("localhost:9097");
        this.streamingApiConfiguration.StreamApiPort.Returns(5000);
        this.streamingApiConfiguration.IntegrateDataFormatManagement.Returns(false);
        this.streamingApiConfiguration.IntegrateSessionManagement.Returns(false);
        this.streamingApiConfiguration.StreamCreationStrategy.Returns(StreamCreationStrategy.PartitionBased);
        this.streamingApiConfiguration.PrometheusMetricPort.Returns(10010);
        this.kafkaChecker = Substitute.For<IKafkaBrokerAvailabilityChecker>();
        this.kafkaChecker.Check(Arg.Any<string>()).Returns(true);
    }

    [Fact]
    public void BeInitialisedAfterInitialiseIsCalled()
    {
        // Arrange
        // Act
        StreamingApiClient.Initialise(this.streamingApiConfiguration, Substitute.For<ICancellationTokenSourceProvider>(), this.kafkaChecker);
        // Assert
        StreamingApiClient.Initialised.Should().BeTrue();
    }

    [Fact]
    public void NotBeInitialisedAfterShutdown()
    {
        // Arrange
        StreamingApiClient.Initialise(this.streamingApiConfiguration, Substitute.For<ICancellationTokenSourceProvider>(), this.kafkaChecker);
        // Act
        StreamingApiClient.Shutdown();
        // Assert
        StreamingApiClient.Initialised.Should().NotBe(true);
    }

    [Fact]
    public void NotBeInitialisedBeforeInitialiseIsCalled()
    {
        // Arrange
        // Act
        // Assert
        StreamingApiClient.Initialised.Should().NotBe(true);
    }

    [Fact]
    public void ReturnClientIfGetConnectionManagerClientCalled()
    {
        // Arrange
        StreamingApiClient.Initialise(this.streamingApiConfiguration, Substitute.For<ICancellationTokenSourceProvider>(), this.kafkaChecker);
        // Act
        var client = StreamingApiClient.GetConnectionManagerClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ReturnClientIfGetDataFormatManagerClientCalled()
    {
        // Arrange
        StreamingApiClient.Initialise(this.streamingApiConfiguration, Substitute.For<ICancellationTokenSourceProvider>(), this.kafkaChecker);
        // Act
        var client = StreamingApiClient.GetDataFormatManagerClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ReturnClientIfGetPacketReaderClientCalled()
    {
        // Arrange
        StreamingApiClient.Initialise(this.streamingApiConfiguration, Substitute.For<ICancellationTokenSourceProvider>(), this.kafkaChecker);
        // Act
        var client = StreamingApiClient.GetPacketReaderClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ReturnClientIfGetPacketWriterClientCalled()
    {
        // Arrange
        StreamingApiClient.Initialise(this.streamingApiConfiguration, Substitute.For<ICancellationTokenSourceProvider>(), this.kafkaChecker);
        // Act
        var client = StreamingApiClient.GetPacketWriterClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ThrowExceptionIfGetConnectionManagerClientCalledBeforeInitialised()
    {
        // Arrange
        // Act
        var act = StreamingApiClient.GetConnectionManagerClient;
        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ThrowExceptionIfGetDataFormatManagerClientCalledBeforeInitialised()
    {
        // Arrange
        // Act
        var act = StreamingApiClient.GetDataFormatManagerClient;
        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ThrowExceptionIfGetPacketReaderClientCalledBeforeInitialised()
    {
        // Arrange
        // Act
        var act = StreamingApiClient.GetPacketReaderClient;
        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ThrowExceptionIfGetPacketWriterClientCalledBeforeInitialised()
    {
        // Arrange
        // Act
        var act = StreamingApiClient.GetPacketWriterClient;
        // Assert
        act.Should().Throw<InvalidOperationException>();
    }
}
