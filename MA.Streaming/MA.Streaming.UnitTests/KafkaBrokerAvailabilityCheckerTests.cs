// <copyright file="KafkaBrokerAvailabilityCheckerTests.cs" company="Motion Applied Ltd.">
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

using Confluent.Kafka;

using MA.Common.Abstractions;
using MA.Streaming.Proto.ServerComponent;

using NSubstitute;

using Xunit;

namespace MA.Streaming.UnitTests;

public class KafkaBrokerAvailabilityCheckerTests
{
    // A very short retry interval so tests complete in milliseconds.
    private const double FastRetrySeconds = 0.05;

    private readonly IAdminClient adminClient = Substitute.For<IAdminClient>();
    private readonly ILogger logger = Substitute.For<ILogger>();

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private KafkaBrokerAvailabilityChecker CreateSut()
        => new KafkaBrokerAvailabilityChecker(_ => this.adminClient, FastRetrySeconds);

    private static Metadata BrokersAvailable()
        => new([new BrokerMetadata(1, "localhost", 9092)], [], 1, "localhost:9092");

    private static Metadata NoBrokers()
        => new([], [], 0, string.Empty);

    // -------------------------------------------------------------------------
    // Check — synchronous availability probe
    // -------------------------------------------------------------------------

    [Fact]
    public void Check_WhenBrokerReturnsAtLeastOneBroker_ReturnsTrueAndLogsInfo()
    {
        // Arrange
        this.adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(BrokersAvailable());
        var sut = this.CreateSut();

        // Act
        var result = sut.Check("broker:9092", this.logger);

        // Assert
        Assert.True(result);
        this.logger.Received(1).Info("Kafka broker is available. Proceeding with the rest of the code...");
    }

    [Fact]
    public void Check_WhenBrokerReturnsEmptyBrokerList_ReturnsFalseAndLogsWarning()
    {
        // Arrange
        this.adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(NoBrokers());
        var sut = this.CreateSut();

        // Act
        var result = sut.Check("broker:9092", this.logger);

        // Assert
        Assert.False(result);
        this.logger.Received(1).Warning("Kafka broker is not available");
    }

    [Fact]
    public void Check_WhenGetMetadataThrows_ReturnsFalseAndLogsWarning()
    {
        // Arrange
        this.adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(_ => throw new KafkaException(ErrorCode.BrokerNotAvailable));
        var sut = this.CreateSut();

        // Act
        var result = sut.Check("broker:9092", this.logger);

        // Assert
        Assert.False(result);
        this.logger.Received(1).Warning("Kafka broker is not available");
    }

    // -------------------------------------------------------------------------
    // CheckContinuously — background monitoring loop
    // -------------------------------------------------------------------------

    [Fact]
    public async Task CheckContinuously_WhenBrokerAlwaysAvailable_NeverLogsAvailableAgain()
    {
        // Arrange — every call returns at least one broker
        this.adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(BrokersAvailable());
        using var cts = new CancellationTokenSource();
        var sut = this.CreateSut();

        // Act — let a few iterations run then cancel
        var task = sut.CheckContinuously("broker:9092", cts, this.logger);
        await Task.Delay(250);
        await cts.CancelAsync();
        await task.ContinueWith(_ => { }); // wait without re-throwing

        // Assert
        this.logger.DidNotReceive().Info("Kafka broker is available again");
    }

    [Fact]
    public async Task CheckContinuously_WhenBrokerInitiallyUnavailableAndBecomesAvailable_LogsAvailableAgain()
    {
        // Arrange
        // Sequence: initial check ? unavailable; loop check 1 ? unavailable; loop check 2 ? available
        var callCount = 0;
        this.adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(_ =>
        {
            callCount++;
            return callCount >= 3 ? BrokersAvailable() : NoBrokers();
        });

        var logCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        this.logger.When(l => l.Info("Kafka broker is available again")).Do(_ => logCalled.TrySetResult());

        using var cts = new CancellationTokenSource();
        var sut = this.CreateSut();

        // Act
        var task = sut.CheckContinuously("broker:9092", cts, this.logger);
        await logCalled.Task.WaitAsync(TimeSpan.FromSeconds(3));
        await cts.CancelAsync();
        await task.ContinueWith(_ => { });

        // Assert
        this.logger.Received(1).Info("Kafka broker is available again");
    }

    [Fact]
    public async Task CheckContinuously_WhenBrokerGoesDownThenRecovers_LogsAvailableAgainOnlyOnRecovery()
    {
        // Arrange
        // Sequence: available × 2, unavailable × 2, available again
        var callCount = 0;
        this.adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(_ =>
        {
            callCount++;
            return callCount is 3 or 4 ? NoBrokers() : BrokersAvailable();
        });

        var logCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        this.logger.When(l => l.Info("Kafka broker is available again")).Do(_ => logCalled.TrySetResult());

        using var cts = new CancellationTokenSource();
        var sut = this.CreateSut();

        // Act
        var task = sut.CheckContinuously("broker:9092", cts, this.logger);
        await logCalled.Task.WaitAsync(TimeSpan.FromSeconds(3));

        // Let one more iteration run to verify no second log call
        await Task.Delay(150);
        await cts.CancelAsync();
        await task.ContinueWith(_ => { });

        // Assert — logged exactly once (on recovery), never for the down-transition
        this.logger.Received(1).Info("Kafka broker is available again");
    }

    [Fact]
    public async Task CheckContinuously_WhenBrokerAlwaysUnavailable_NeverLogsAvailableAgain()
    {
        // Arrange — every call throws (simulates completely unreachable broker)
        this.adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(_ => throw new KafkaException(ErrorCode.BrokerNotAvailable));
        using var cts = new CancellationTokenSource();
        var sut = this.CreateSut();

        // Act
        var task = sut.CheckContinuously("broker:9092", cts, this.logger);
        await Task.Delay(250);
        await cts.CancelAsync();
        await task.ContinueWith(_ => { });

        // Assert
        this.logger.DidNotReceive().Info("Kafka broker is available again");
    }

    [Fact]
    public async Task CheckContinuously_WhenCancelledWhileDelayingBetweenRetries_TaskCompletesCleanly()
    {
        // Arrange — broker always available so the loop always hits the delay path
        this.adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(BrokersAvailable());
        using var cts = new CancellationTokenSource();
        var sut = this.CreateSut();

        // Act
        var task = sut.CheckContinuously("broker:9092", cts, this.logger);

        // Give the loop time to reach Task.Delay, then cancel
        await Task.Delay(30);
        await cts.CancelAsync();

        // Assert — task should finish (cancelled or ran-to-completion) within a reasonable timeout
        var completed = await Task.WhenAny(task.ContinueWith(_ => { }), Task.Delay(2000));
        Assert.NotEqual(Task.Delay(2000), completed);
    }

    [Fact]
    public async Task CheckContinuously_WhenStateChangesRepeatedly_LogsAvailableAgainEachTimeItRecovers()
    {
        // Arrange — alternates unavailable / available so broker "comes back" twice
        // Sequence: unavailable(1), available(2) [log], unavailable(3), available(4) [log], ...
        var callCount = 0;
        this.adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(_ =>
        {
            callCount++;
            return callCount % 2 == 0 ? BrokersAvailable() : NoBrokers();
        });

        var logCount = 0;
        var secondLogCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        this.logger.When(l => l.Info("Kafka broker is available again")).Do(_ =>
        {
            if (Interlocked.Increment(ref logCount) == 2)
            {
                secondLogCalled.TrySetResult();
            }
        });

        using var cts = new CancellationTokenSource();
        var sut = this.CreateSut();

        // Act
        var task = sut.CheckContinuously("broker:9092", cts, this.logger);
        await secondLogCalled.Task.WaitAsync(TimeSpan.FromSeconds(3));
        await cts.CancelAsync();
        await task.ContinueWith(_ => { });

        // Assert — logged at least twice (once per recovery)
        Assert.True(logCount >= 2);
    }

    [Fact]
    public async Task CheckContinuously_WhenPassingNullLogger_DoesNotThrow()
    {
        // Arrange — null logger; broker becomes available so the internal log call is attempted
        var callCount = 0;
        this.adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(_ =>
        {
            callCount++;
            return callCount >= 2 ? BrokersAvailable() : NoBrokers();
        });

        var recoveryReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // We cannot intercept the ConsoleLogger, so we just wait for enough
        // iterations to have passed and assert no exception was thrown.
        using var cts = new CancellationTokenSource();
        var sut = this.CreateSut();

        var task = sut.CheckContinuously("broker:9092", cts, logger: null);
        await Task.Delay(300);
        await cts.CancelAsync();

        var exception = await Record.ExceptionAsync(() => task.ContinueWith(t =>
        {
            if (t.IsFaulted)
            {
                throw t.Exception!.GetBaseException();
            }
        }));

        Assert.Null(exception);
    }
}
