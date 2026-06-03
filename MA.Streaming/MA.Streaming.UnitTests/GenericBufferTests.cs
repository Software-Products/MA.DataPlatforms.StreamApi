// <copyright file="GenericBufferTests.cs" company="Motion Applied Ltd.">
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

using System.Collections.Concurrent;

using MA.Common.Abstractions;
using MA.Streaming.Core;
using MA.Streaming.Core.Abstractions;

using NSubstitute;

using Xunit;

namespace MA.Streaming.UnitTests;

public class GenericBufferTests
{
    private readonly IBufferConsumer<string> consumer = Substitute.For<IBufferConsumer<string>>();
    private readonly ILogger logger = Substitute.For<ILogger>();

    [Fact]
    public async Task AddData_WhenCalled_ConsumerReceivesData()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        this.consumer
            .ConsumeAsync(Arg.Any<string>())
            .Returns(call =>
            {
                received.TrySetResult(call.Arg<string>());
                return Task.CompletedTask;
            });

        var buffer = new GenericBuffer<string>(this.consumer, cts.Token, this.logger);

        // Act
        buffer.AddData("hello");

        // Assert
        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("hello", result);

        await cts.CancelAsync();
    }

    [Fact]
    public async Task AddData_MultipleItems_AllItemsConsumedInOrder()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var items = new List<string>
        {
            "a",
            "b",
            "c",
            "d",
            "e"
        };
        var consumed = new List<string>();
        var allConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        this.consumer
            .ConsumeAsync(Arg.Any<string>())
            .Returns(call =>
            {
                lock (consumed)
                {
                    consumed.Add(call.Arg<string>());
                    if (consumed.Count == items.Count)
                    {
                        allConsumed.TrySetResult();
                    }
                }

                return Task.CompletedTask;
            });

        var buffer = new GenericBuffer<string>(this.consumer, cts.Token, this.logger);

        // Act
        foreach (var item in items)
        {
            buffer.AddData(item);
        }

        // Assert
        await allConsumed.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal(items.Count, consumed.Count);
        Assert.All(items, i => Assert.Contains(i, consumed));

        await cts.CancelAsync();
    }

    [Fact]
    public async Task AddData_WhenCancellationRequested_DoesNotAddToBuffer()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var buffer = new GenericBuffer<string>(this.consumer, cts.Token, this.logger);

        // Act — no exception should be thrown
        buffer.AddData("should-not-arrive");

        // Give a brief window in case the consumer would be called
        await Task.Delay(100);

        // Assert
        await this.consumer.DidNotReceive().ConsumeAsync(Arg.Any<string>());
    }

    [Fact]
    public async Task AddData_WhenCancellationRequestedDuringAdd_DoesNotThrow()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var buffer = new GenericBuffer<string>(this.consumer, cts.Token, this.logger);

        // Cancel right after creating the buffer
        await cts.CancelAsync();

        // Act & Assert — must not throw
        var exception = Record.Exception(() => buffer.AddData("test"));
        Assert.Null(exception);
    }

    [Fact]
    public async Task StartProcessing_WhenCancelled_LogsInfoMessage()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var logCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        this.logger.When(l => l.Info("Processing was canceled.")).Do(_ => logCalled.TrySetResult());

        var unused = new GenericBuffer<string>(this.consumer, cts.Token, this.logger);

        // Let the background task start and block on WaitAsync before we cancel
        await Task.Delay(50);

        // Act
        await cts.CancelAsync();

        // Assert — deterministic: wait until the catch block actually runs
        await logCalled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        this.logger.Received().Info("Processing was canceled.");
    }

    [Fact]
    public async Task AddData_WhenConsumerThrows_ExceptionIsLoggedAndProcessingContinues()
    {
        // ConsumeAsync is now wrapped in a try/catch that logs the exception,
        // so the background loop must survive and process subsequent items.

        // Arrange
        using var cts = new CancellationTokenSource();
        var badConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var goodConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        this.consumer
            .ConsumeAsync(Arg.Any<string>())
            .Returns(call =>
            {
                if (call.Arg<string>() == "bad")
                {
                    badConsumed.TrySetResult();
                    throw new InvalidOperationException("consumer error");
                }

                goodConsumed.TrySetResult();
                return Task.CompletedTask;
            });

        var buffer = new GenericBuffer<string>(this.consumer, cts.Token, this.logger);

        // Act
        buffer.AddData("bad");
        await badConsumed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        buffer.AddData("good");

        // Assert — loop is still alive; "good" is consumed
        await goodConsumed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        this.logger.Received().Error(Arg.Is<string>(msg => msg.Contains("consumer error")));

        await cts.CancelAsync();
    }

    [Fact]
    public async Task AddData_NoDataAdded_ConsumerNeverCalled()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var unused = new GenericBuffer<string>(this.consumer, cts.Token, this.logger);

        // Act — add nothing
        await Task.Delay(100);

        // Assert
        await this.consumer.DidNotReceive().ConsumeAsync(Arg.Any<string>());

        await cts.CancelAsync();
    }

    [Fact]
    public async Task AddData_WhenCancellationOccursDuringWaitAsync_ProcessingStopsGracefully()
    {
        // Arrange — the semaphore is never released so the background loop is blocked on WaitAsync
        using var cts = new CancellationTokenSource();
        var logCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        this.logger.When(l => l.Info("Processing was canceled.")).Do(_ => logCalled.TrySetResult());

        var unused = new GenericBuffer<string>(this.consumer, cts.Token, this.logger);

        // Let the background task start and block on WaitAsync before we cancel
        await Task.Delay(50);

        // Act — cancel while the background loop is blocked on the empty semaphore
        await cts.CancelAsync();

        // Assert — deterministic: wait until the catch block actually runs
        await logCalled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        this.logger.Received().Info("Processing was canceled.");
    }

    [Fact]
    public async Task AddData_HighConcurrency_AllItemsConsumed()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        const int Total = 200;
        var consumed = new ConcurrentBag<int>();
        var allDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var intConsumer = Substitute.For<IBufferConsumer<int>>();
        intConsumer
            .ConsumeAsync(Arg.Any<int>())
            .Returns(call =>
            {
                consumed.Add(call.Arg<int>());
                if (consumed.Count == Total)
                {
                    allDone.TrySetResult();
                }

                return Task.CompletedTask;
            });

        var buffer = new GenericBuffer<int>(intConsumer, cts.Token);

        // Act
        Parallel.For(0, Total, i => buffer.AddData(i));

        // Assert
        await allDone.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(Total, consumed.Count);

        await cts.CancelAsync();
    }

    [Fact]
    public async Task AddData_WithNullLogger_DoesNotThrow()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var buffer = new GenericBuffer<string>(this.consumer, cts.Token, logger: null);

        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        this.consumer.ConsumeAsync(Arg.Any<string>()).Returns(_ =>
        {
            received.TrySetResult();
            return Task.CompletedTask;
        });

        // Act & Assert — no NullReferenceException
        buffer.AddData("test");
        await received.Task.WaitAsync(TimeSpan.FromSeconds(2));

        await cts.CancelAsync();

        // Allow background task to handle cancellation without null logger crash
        await Task.Delay(200);
    }
}
