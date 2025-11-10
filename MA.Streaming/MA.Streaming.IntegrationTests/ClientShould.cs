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

public class ClientShould : IClassFixture<StreamApiTestsCleanUpFixture>
{
    private readonly IStreamingApiConfiguration streamingApiConfiguration;
    private readonly IKafkaBrokerAvailabilityChecker kafkaChecker;
    private readonly ILoggingDirectoryProvider loggingDirectoryProvider;

    public ClientShould(StreamApiTestsCleanUpFixture _)
    {
        // Ensure each test run starts from as "clean" a state as possible
        StreamingApiClient.Shutdown();
        this.streamingApiConfiguration = Substitute.For<IStreamingApiConfiguration>();
        this.loggingDirectoryProvider = new LoggingDirectoryProvider("");
        this.streamingApiConfiguration.BrokerUrl.Returns("localhost:9097");
        this.streamingApiConfiguration.StreamApiPort.Returns(5000);
        this.streamingApiConfiguration.IntegrateDataFormatManagement.Returns(false);
        this.streamingApiConfiguration.IntegrateSessionManagement.Returns(false);
        this.streamingApiConfiguration.StreamCreationStrategy.Returns(StreamCreationStrategy.PartitionBased);
        this.kafkaChecker = Substitute.For<IKafkaBrokerAvailabilityChecker>();
        this.kafkaChecker.Check(Arg.Any<string>()).Returns(true);
    }

    [Fact]
    public void BeInitialisedAfterInitialiseIsCalled()
    {
        // Arrange
        var initialisedEventCounter = 0;
        StreamingApiClient.ServiceInitialised += (_, _) => initialisedEventCounter++;
        // Act
        StreamingApiClient.Initialise(
            this.streamingApiConfiguration,
            CreateCancellationTokenSourceProvider(),
            this.kafkaChecker,
            this.loggingDirectoryProvider);
        // Assert
        StreamingApiClient.Initialised.Should().BeTrue();
        initialisedEventCounter.Should().Be(1);
    }

    [Fact]
    public void NotBeInitialisedAfterShutdown()
    {
        // Arrange
        StreamingApiClient.Initialise(
            this.streamingApiConfiguration,
            CreateCancellationTokenSourceProvider(),
            this.kafkaChecker,
            this.loggingDirectoryProvider);
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
        StreamingApiClient.Initialise(
            this.streamingApiConfiguration,
            CreateCancellationTokenSourceProvider(),
            this.kafkaChecker,
            this.loggingDirectoryProvider);
        // Act
        var client = StreamingApiClient.GetConnectionManagerClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ReturnClientIfGetDataFormatManagerClientCalled()
    {
        // Arrange
        StreamingApiClient.Initialise(
            this.streamingApiConfiguration,
            CreateCancellationTokenSourceProvider(),
            this.kafkaChecker,
            this.loggingDirectoryProvider);
        // Act
        var client = StreamingApiClient.GetDataFormatManagerClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ReturnClientIfGetPacketReaderClientCalled()
    {
        // Arrange
        StreamingApiClient.Initialise(
            this.streamingApiConfiguration,
            CreateCancellationTokenSourceProvider(),
            this.kafkaChecker,
            this.loggingDirectoryProvider);
        // Act
        var client = StreamingApiClient.GetPacketReaderClient();
        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public void ReturnClientIfGetPacketWriterClientCalled()
    {
        // Arrange
        StreamingApiClient.Initialise(
            this.streamingApiConfiguration,
            CreateCancellationTokenSourceProvider(),
            this.kafkaChecker,
            this.loggingDirectoryProvider);
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

    [Fact]
    public async Task BeInitialisedWhenInitialisedAsyncIsCalled()
    {
        // Arrange
        var initialisedEventCounter = 0;
        StreamingApiClient.ServiceInitialised += (_, _) => initialisedEventCounter++;
        var cts = new CancellationTokenSource();

        // Act
        await StreamingApiClient.InitialiseAsync(
            this.streamingApiConfiguration,
            CreateCancellationTokenSourceProvider(),
            this.kafkaChecker,
            this.loggingDirectoryProvider,
            cts.Token);

        // Assert
        StreamingApiClient.Initialised.Should().BeTrue();
        initialisedEventCounter.Should().Be(1);
    }

    [Fact]
    public void NotBeInitialisedWhenInitialisedAsyncIsCancelled()
    {
        // Arrange
        var initialisedEventCounter = 0;
        this.kafkaChecker.Check(Arg.Any<string>()).Returns(false);
        var autoResetEvent = new AutoResetEvent(false);
        StreamingApiClient.ServiceInitialised += (_, _) =>
        {
            initialisedEventCounter++;
            autoResetEvent.Set();
        };
        var cts = new CancellationTokenSource();

        // Act
        Task.Run(async () =>
        {
            await StreamingApiClient.InitialiseAsync(
                this.streamingApiConfiguration,
                CreateCancellationTokenSourceProvider(),
                this.kafkaChecker,
                this.loggingDirectoryProvider,
                cts.Token);
        });

        autoResetEvent.WaitOne(TimeSpan.FromSeconds(1));
        cts.Cancel();

        // Assert
        StreamingApiClient.Initialised.Should().BeFalse();
        initialisedEventCounter.Should().Be(0);
    }

    private static ICancellationTokenSourceProvider CreateCancellationTokenSourceProvider()
    {
        var cancellationTokenSourceProvider = Substitute.For<ICancellationTokenSourceProvider>();
        cancellationTokenSourceProvider.Provide().Returns(new CancellationTokenSource());
        return cancellationTokenSourceProvider;
    }
}
