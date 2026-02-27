// <copyright file="ServerTests.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Configs;
using MA.Streaming.Proto.ServerComponent;

using NSubstitute;

using Xunit;

namespace MA.Streaming.UnitTests;

public class ServerTests
{
    [Fact]
    public void ValidateStreamApiConfiguration_WhenBatchingSizeIsWithinLimits_ReturnsOriginalConfiguration()
    {
        // Arrange
        var streamingApiConfiguration = CreateStreamingApiConfiguration(batchingSizesKb: 1024); // 1 MB
        var cancellationTokenSourceProvider = Substitute.For<ICancellationTokenSourceProvider>();
        var kafkaBrokerAvailabilityChecker = Substitute.For<IKafkaBrokerAvailabilityChecker>();
        var loggingDirectoryProvider = Substitute.For<ILoggingDirectoryProvider>();
        const int GrpcMaxSendMessageSize = 4 * 1024 * 1024; // 4 MB
        const int GrpcMaxReceiveMessageSize = 4 * 1024 * 1024; // 4 MB

        // Act
        var server = new Server(
            streamingApiConfiguration,
            cancellationTokenSourceProvider,
            kafkaBrokerAvailabilityChecker,
            loggingDirectoryProvider,
            grpcMaxSendMessageSize: GrpcMaxSendMessageSize,
            grpcMaxReceiveMessageSize: GrpcMaxReceiveMessageSize);

        // Assert
        // The configuration should remain unchanged (validated via constructor)
        Assert.NotNull(server);
    }

    [Fact]
    public void ValidateStreamApiConfiguration_WhenBatchingSizeExceedsSendLimit_ReducesBatchingSize()
    {
        // Arrange
        var logger = Substitute.For<ILogger>();
        var streamingApiConfiguration = CreateStreamingApiConfiguration(batchingSizesKb: 5 * 1024); // 5 MB
        var cancellationTokenSourceProvider = Substitute.For<ICancellationTokenSourceProvider>();
        var kafkaBrokerAvailabilityChecker = Substitute.For<IKafkaBrokerAvailabilityChecker>();
        var loggingDirectoryProvider = Substitute.For<ILoggingDirectoryProvider>();
        const int GrpcMaxSendMessageSize = 4 * 1024 * 1024; // 4 MB
        const int GrpcMaxReceiveMessageSize = 4 * 1024 * 1024; // 4 MB

        // Act
        _ = new Server(
            streamingApiConfiguration,
            cancellationTokenSourceProvider,
            kafkaBrokerAvailabilityChecker,
            loggingDirectoryProvider,
            logger: logger,
            grpcMaxSendMessageSize: GrpcMaxSendMessageSize,
            grpcMaxReceiveMessageSize: GrpcMaxReceiveMessageSize);

        // Assert
        logger.Received(1).Warning(
            Arg.Is<string>(msg =>
                msg.Contains("batching size bigger than gRPC max message size")));
        logger.Received(1).Info(
            Arg.Is<string>(msg =>
                msg.Contains("batching size reduced to gRPC max message size: 4096 KB")));
    }

    [Fact]
    public void ValidateStreamApiConfiguration_WhenBatchingSizeExceedsReceiveLimit_ReducesBatchingSize()
    {
        // Arrange
        var logger = Substitute.For<ILogger>();
        var streamingApiConfiguration = CreateStreamingApiConfiguration(batchingSizesKb: 5 * 1024); // 5 MB
        var cancellationTokenSourceProvider = Substitute.For<ICancellationTokenSourceProvider>();
        var kafkaBrokerAvailabilityChecker = Substitute.For<IKafkaBrokerAvailabilityChecker>();
        var loggingDirectoryProvider = Substitute.For<ILoggingDirectoryProvider>();
        const int GrpcMaxSendMessageSize = 6 * 1024 * 1024; // 6 MB
        const int GrpcMaxReceiveMessageSize = 3 * 1024 * 1024; // 3 MB

        // Act
        _ = new Server(
            streamingApiConfiguration,
            cancellationTokenSourceProvider,
            kafkaBrokerAvailabilityChecker,
            loggingDirectoryProvider,
            logger: logger,
            grpcMaxSendMessageSize: GrpcMaxSendMessageSize,
            grpcMaxReceiveMessageSize: GrpcMaxReceiveMessageSize);

        // Assert
        logger.Received(1).Warning(
            Arg.Is<string>(msg =>
                msg.Contains("batching size bigger than gRPC max message size")));
        logger.Received(1).Info(
            Arg.Is<string>(msg =>
                msg.Contains("batching size reduced to gRPC max message size: 3072 KB")));
    }

    [Fact]
    public void ValidateStreamApiConfiguration_WhenBothLimitsExceeded_UsesMinimumLimit()
    {
        // Arrange
        var logger = Substitute.For<ILogger>();
        var streamingApiConfiguration = CreateStreamingApiConfiguration(batchingSizesKb: 5 * 1024); // 5 MB
        var cancellationTokenSourceProvider = Substitute.For<ICancellationTokenSourceProvider>();
        var kafkaBrokerAvailabilityChecker = Substitute.For<IKafkaBrokerAvailabilityChecker>();
        var loggingDirectoryProvider = Substitute.For<ILoggingDirectoryProvider>();
        const int GrpcMaxSendMessageSize = 2 * 1024 * 1024; // 2 MB
        const int GrpcMaxReceiveMessageSize = 3 * 1024 * 1024; // 3 MB

        // Act
        _ = new Server(
            streamingApiConfiguration,
            cancellationTokenSourceProvider,
            kafkaBrokerAvailabilityChecker,
            loggingDirectoryProvider,
            logger: logger,
            grpcMaxSendMessageSize: GrpcMaxSendMessageSize,
            grpcMaxReceiveMessageSize: GrpcMaxReceiveMessageSize);

        // Assert
        logger.Received(1).Warning(Arg.Any<string>());
        logger.Received(1).Info(
            Arg.Is<string>(msg =>
                msg.Contains("batching size reduced to gRPC max message size: 2048 KB")));
    }

    [Fact]
    public void ValidateStreamApiConfiguration_WhenBatchingSizeIsLessThanBothLimits_ReturnsOriginalConfiguration()
    {
        // Arrange
        var logger = Substitute.For<ILogger>();
        var streamingApiConfiguration = CreateStreamingApiConfiguration(batchingSizesKb: 1024); // 1 MB
        var cancellationTokenSourceProvider = Substitute.For<ICancellationTokenSourceProvider>();
        var kafkaBrokerAvailabilityChecker = Substitute.For<IKafkaBrokerAvailabilityChecker>();
        var loggingDirectoryProvider = Substitute.For<ILoggingDirectoryProvider>();
        const int GrpcMaxSendMessageSize = 4 * 1024 * 1024; // 4 MB
        const int GrpcMaxReceiveMessageSize = 4 * 1024 * 1024; // 4 MB

        // Act
        var server = new Server(
            streamingApiConfiguration,
            cancellationTokenSourceProvider,
            kafkaBrokerAvailabilityChecker,
            loggingDirectoryProvider,
            logger: logger,
            grpcMaxSendMessageSize: GrpcMaxSendMessageSize,
            grpcMaxReceiveMessageSize: GrpcMaxReceiveMessageSize);

        // Assert
        Assert.NotNull(server);
        logger.DidNotReceive().Warning(Arg.Any<string>());
        logger.DidNotReceive().Info(Arg.Any<string>());
    }

    private static StreamingApiConfiguration CreateStreamingApiConfiguration(int batchingSizesKb)
    {
        return new StreamingApiConfiguration(
            streamCreationStrategy: StreamCreationStrategy.TopicBased,
            brokerUrl: "localhost:9092",
            partitionMappings: null,
            streamApiPort: 13579,
            integrateSessionManagement: true,
            integrateDataFormatManagement: true,
            useRemoteKeyGenerator: false,
            remoteKeyGeneratorServiceAddress: "",
            batchingResponses: false,
            initialisationTimeoutSeconds: 3,
            terminationTimeoutSeconds: 1,
            domain: "test",
            batchingSizesKb: batchingSizesKb,
            batchingTimeMs: 100);
    }
}
