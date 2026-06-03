// <copyright file="ClientShould.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Core;
using MA.Streaming.IntegrationTests.Base;
using MA.Streaming.Proto.Client.Local;

using NSubstitute;

using Xunit;

namespace MA.Streaming.IntegrationTests;

public class ClientShould
{
    private readonly IKafkaBrokerAvailabilityChecker kafkaChecker;

    private readonly StreamingApiClient streamingApiClient;

    public ClientShould()
    {
        var streamingApiConfiguration = Substitute.For<IStreamingApiConfiguration>();
        ILoggingDirectoryProvider loggingDirectoryProvider1 = new LoggingDirectoryProvider("");
        streamingApiConfiguration.BrokerUrl.Returns("localhost:9097");
        streamingApiConfiguration.StreamApiPort.Returns(5000);
        streamingApiConfiguration.IntegrateDataFormatManagement.Returns(false);
        streamingApiConfiguration.IntegrateSessionManagement.Returns(false);
        streamingApiConfiguration.StreamCreationStrategy.Returns(StreamCreationStrategy.PartitionBased);
        streamingApiConfiguration.StreamApiPort.Returns(ApiPortCreator.CreateNew());
        streamingApiConfiguration.Domain.Returns("ClientShouldDomain");
        this.kafkaChecker = Substitute.For<IKafkaBrokerAvailabilityChecker>();
        this.kafkaChecker.Check(Arg.Any<string>(),Arg.Any<ILogger?>()).Returns(true);
        this.streamingApiClient = StreamingApiClientFactory.Create(
            streamingApiConfiguration,
            CreateCancellationTokenSourceProvider(),
            this.kafkaChecker,
            loggingDirectoryProvider1);
    }

    [Fact]
    public void BeInitialisedAfterInitialiseIsCalled()
    {
        // Arrange
        var initialisedEventCounter = 0;
        this.streamingApiClient.ServiceInitialised += (_, _) => initialisedEventCounter++;
        // Act
        this.streamingApiClient.Initialise();
        // Assert
        this.streamingApiClient.Initialised.Should().BeTrue();
        initialisedEventCounter.Should().Be(1);
    }

    [Fact]
    public void NotBeInitialisedAfterShutdown()
    {
        // Arrange
        this.streamingApiClient.Initialise();
        // Act
        this.streamingApiClient.Shutdown();
        // Assert
        this.streamingApiClient.Initialised.Should().NotBe(true);
    }

    [Fact]
    public void NotBeInitialisedBeforeInitialiseIsCalled()
    {
        // Arrange
        // Act
        // Assert
        this.streamingApiClient.Initialised.Should().NotBe(true);
    }

    [Fact]
    public void ReturnClientIfGetConnectionManagerClientCalled()
    {
        // Arrange
        this.streamingApiClient.Initialise();
        // Act
        var client = this.streamingApiClient.GetConnectionManagerClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ReturnClientIfGetDataFormatManagerClientCalled()
    {
        // Arrange
        this.streamingApiClient.Initialise();
        // Act
        var client = this.streamingApiClient.GetDataFormatManagerClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ReturnClientIfGetPacketReaderClientCalled()
    {
        // Arrange
        this.streamingApiClient.Initialise();
        // Act
        var client = this.streamingApiClient.GetPacketReaderClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ReturnClientIfGetPacketWriterClientCalled()
    {
        // Arrange
        this.streamingApiClient.Initialise();
        // Act
        var client = this.streamingApiClient.GetPacketWriterClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ThrowExceptionIfGetConnectionManagerClientCalledBeforeInitialised()
    {
        // Arrange
        // Act
        var act = this.streamingApiClient.GetConnectionManagerClient;
        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ThrowExceptionIfGetDataFormatManagerClientCalledBeforeInitialised()
    {
        // Arrange
        // Act
        var act = this.streamingApiClient.GetDataFormatManagerClient;
        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ThrowExceptionIfGetPacketReaderClientCalledBeforeInitialised()
    {
        // Arrange
        // Act
        var act = this.streamingApiClient.GetPacketReaderClient;
        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ThrowExceptionIfGetPacketWriterClientCalledBeforeInitialised()
    {
        // Arrange
        // Act
        var act = this.streamingApiClient.GetPacketWriterClient;
        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task BeInitialisedWhenInitialisedAsyncIsCalled()
    {
        // Arrange
        var initialisedEventCounter = 0;
        this.streamingApiClient.ServiceInitialised += (_, _) => initialisedEventCounter++;

        // Act
        await this.streamingApiClient.InitialiseAsync();

        // Assert
        this.streamingApiClient.Initialised.Should().BeTrue();
        initialisedEventCounter.Should().Be(1);
    }

    [Fact]
    public void NotBeInitialisedWhenInitialisedAsyncIsCancelled()
    {
        // Arrange
        var initialisedEventCounter = 0;
        this.kafkaChecker.Check(Arg.Any<string>(),Arg.Any<ILogger?>()).Returns(false);
        var autoResetEvent = new AutoResetEvent(false);
        this.streamingApiClient.ServiceInitialised += (_, _) =>
        {
            initialisedEventCounter++;
            autoResetEvent.Set();
        };
        var cts = new CancellationTokenSource();

        // Act
        Task.Run(
            async () =>
            {
                await this.streamingApiClient.InitialiseAsync();
            },
            cts.Token);

        autoResetEvent.WaitOne(TimeSpan.FromSeconds(1));
        cts.Cancel();

        // Assert
        this.streamingApiClient.Initialised.Should().BeFalse();
        initialisedEventCounter.Should().Be(0);
    }

    private static ICancellationTokenSourceProvider CreateCancellationTokenSourceProvider()
    {
        var cancellationTokenSourceProvider = Substitute.For<ICancellationTokenSourceProvider>();
        cancellationTokenSourceProvider.Provide().Returns(new CancellationTokenSource());
        return cancellationTokenSourceProvider;
    }
}
