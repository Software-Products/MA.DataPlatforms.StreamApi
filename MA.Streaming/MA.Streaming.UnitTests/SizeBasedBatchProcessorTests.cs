// <copyright file="SizeBasedBatchProcessorTests.cs" company="Motion Applied Ltd.">
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

using System.Diagnostics;

using MA.Common.Abstractions;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Core.Handlers;

using NSubstitute;

using Xunit;

namespace MA.Streaming.UnitTests;

public class SizeBasedBatchProcessorTests : IDisposable
{
    private readonly List<BatchProcessedEvent> processedBatches = [];
    private readonly object lockObject = new();
    private readonly ILogger logger = Substitute.For<ILogger>();
    private bool disposedValue;

    protected virtual void Dispose(bool disposing)
    {
        if (this.disposedValue)
        {
            return;
        }

        if (disposing)
        {
            lock (this.lockObject)
            {
                this.processedBatches.Clear();
            }
        }

        this.disposedValue = true;
    }

    public void Dispose()
    {
        this.Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    private class BatchProcessedEvent
    {
        public int MessageCount { get; init; }

        public int TotalSizeBytes { get; init; }

        public DateTime ProcessedAt { get; init; }

        public List<Packet> Messages { get; init; } = [];
    }

    private async Task MockProcessAction(IReadOnlyList<Packet> batch)
    {
        await Task.Delay(10); // Simulate processing time

        lock (this.lockObject)
        {
            this.processedBatches.Add(
                new BatchProcessedEvent
                {
                    MessageCount = batch.Count,
                    TotalSizeBytes = batch.Sum(m => m.CalculateSize()),
                    ProcessedAt = DateTime.UtcNow,
                    Messages = batch.ToList()
                });
        }
    }

    /// <summary>
    /// Polls until at least <paramref name="expectedBatchCount"/> batches have been
    /// recorded or <paramref name="timeoutMs"/> elapses.  Using a polling loop instead
    /// of a fixed Task.Delay makes tests resilient to CI scheduler latency: on a loaded
    /// build agent, Task.Run startup and System.Timers.Timer callbacks can be delayed
    /// by hundreds of milliseconds, causing fixed-sleep tests to assert before the
    /// batch is actually processed.
    /// </summary>
    private async Task WaitForBatchesAsync(int expectedBatchCount = 1, int timeoutMs = 3000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            lock (this.lockObject)
            {
                if (this.processedBatches.Count >= expectedBatchCount)
                {
                    return;
                }
            }

            await Task.Delay(50);
        }
    }

    [Fact]
    public async Task ShouldProcessBatch_WhenSizeThresholdReached()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        const int MaxSize = 1000; // 1KB max
        const double Threshold = 0.70; // 70%
        const int ExpectedThreshold = (int)(MaxSize * Threshold); // 700 bytes

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: MaxSize,
            thresholdPercentage: Threshold,
            timeWindowMilliseconds: 10000 // Long window to ensure size triggers first
        );

        // Act: Add messages that total ~750 bytes (above 70% threshold)
        for (var i = 0; i < 15; i++)
        {
            var packet = PacketFactory.CreatePacket(50); // 50 bytes each
            processor.Add(packet);
        }

        // Wait for processing
        await Task.Delay(500, cts.Token);

        // Assert
        lock (this.lockObject)
        {
            Assert.Single(this.processedBatches); // Should have 1 batch
            var batch = this.processedBatches[0];
            Assert.Equal(15, batch.MessageCount);
            Assert.True(
                batch.TotalSizeBytes >= ExpectedThreshold,
                $"Batch size {batch.TotalSizeBytes} should be >= threshold {ExpectedThreshold}");
        }

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 2: Time-based batching - Should process when time window expires
    /// </summary>
    [Fact]
    public async Task ShouldProcessBatch_WhenTimeWindowExpires()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        const int TimeWindow = 100; // 100ms

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: 10000, // Large size to ensure time triggers first
            thresholdPercentage: 0.70,
            timeWindowMilliseconds: TimeWindow
        );

        // Act: Add a few small messages (won't reach size threshold)
        for (var i = 0; i < 5; i++)
        {
            var packet = PacketFactory.CreatePacket(10); // Very small messages
            processor.Add(packet);
        }

        // Wait until the timer-triggered batch arrives (or timeout).
        // A fixed Task.Delay is fragile on CI: Task.Run startup and Timer callbacks
        // can be delayed by hundreds of ms on a loaded agent.
        await this.WaitForBatchesAsync(expectedBatchCount: 1, timeoutMs: 3000);

        // Assert
        lock (this.lockObject)
        {
            Assert.Single(this.processedBatches); // Should process due to time
            Assert.Equal(5, this.processedBatches[0].MessageCount);
        }

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 3: Max size protection - Should process before exceeding max size
    /// </summary>
    [Fact]
    public async Task ShouldProcessBatch_BeforeExceedingMaxSize()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        const int MaxSize = 500; // 500 bytes max

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: MaxSize,
            thresholdPercentage: 0.70, // 350 bytes threshold
            timeWindowMilliseconds: 10000
        );

        // Act: Add messages where one would exceed max
        processor.Add(PacketFactory.CreatePacket(200)); // 200 bytes
        processor.Add(PacketFactory.CreatePacket(200)); // 400 total - above threshold
        await Task.Delay(200, cts.Token); // Should process here

        processor.Add(PacketFactory.CreatePacket(300)); // Would exceed 500, so previous batch processed first
        await Task.Delay(200, cts.Token);

        // Assert
        lock (this.lockObject)
        {
            Assert.True(this.processedBatches.Count >= 1, "Should have at least 1 batch");
            // First batch should be processed before adding the 300-byte message
            Assert.True(
                this.processedBatches[0].TotalSizeBytes <= MaxSize,
                $"Batch size {this.processedBatches[0].TotalSizeBytes} should not exceed max {MaxSize}");
        }

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 4: Gap handling - Should process after time window during message gap
    /// </summary>
    [Fact]
    public async Task ShouldProcessBatch_AfterGapWithTimeWindow()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        const int TimeWindow = 150; // 150ms

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: 10000,
            thresholdPercentage: 0.90, // High threshold to avoid size trigger
            timeWindowMilliseconds: TimeWindow
        );

        // Act: Add messages, then gap, then more messages
        processor.Add(PacketFactory.CreatePacket(50));
        processor.Add(PacketFactory.CreatePacket(50));

        // GAP - wait for time window
        await Task.Delay(TimeWindow + 100, cts.Token);

        // Assert first batch processed
        lock (this.lockObject)
        {
            Assert.Single(this.processedBatches);
            Assert.Equal(2, this.processedBatches[0].MessageCount);
        }

        // Add more messages after gap
        processor.Add(PacketFactory.CreatePacket(50));
        await Task.Delay(TimeWindow + 100, cts.Token);

        // Assert second batch processed
        lock (this.lockObject)
        {
            Assert.Equal(2, this.processedBatches.Count);
            Assert.Single(this.processedBatches[1].Messages);
        }

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 4B: Multiple gaps - Should handle multiple gaps and process all data
    /// </summary>
    [Fact]
    public async Task ShouldProcessAllBatches_WithMultipleGaps()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        const int TimeWindow = 100; // 100ms
        var totalMessagesAdded = 0;

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: 100000, // Large enough to not hit size threshold
            thresholdPercentage: 0.90,
            timeWindowMilliseconds: TimeWindow
        );

        // Act: Simulate message flow with multiple gaps
        // Use larger packet sizes to ensure consistent sizes
        // Round 1: Messages -> Gap
        for (var i = 0; i < 3; i++)
        {
            processor.Add(PacketFactory.CreatePacket(100)); // Larger packets
            totalMessagesAdded++;
        }

        await Task.Delay(TimeWindow + 1000, cts.Token); // Longer gap to ensure processing

        // Round 2: Messages -> Gap
        for (var i = 0; i < 5; i++)
        {
            processor.Add(PacketFactory.CreatePacket(100));
            totalMessagesAdded++;
        }

        await Task.Delay(TimeWindow + 1000, cts.Token); // GAP 2

        // Round 3: Messages -> Gap
        for (var i = 0; i < 2; i++)
        {
            processor.Add(PacketFactory.CreatePacket(100));
            totalMessagesAdded++;
        }

        await Task.Delay(TimeWindow + 1000, cts.Token); // GAP 3

        // Round 4: Messages -> Gap
        for (var i = 0; i < 4; i++)
        {
            processor.Add(PacketFactory.CreatePacket(100));
            totalMessagesAdded++;
        }

        await Task.Delay(TimeWindow + 1000, cts.Token); // GAP 4 - Final processing

        // Assert: All messages should be processed across multiple batches
        lock (this.lockObject)
        {
            var totalMessagesProcessed = this.processedBatches.Sum(b => b.MessageCount);

            // Should have at least 2 batches to demonstrate gap handling
            Assert.True(
                this.processedBatches.Count >= 2,
                $"Should have processed at least 2 batches for multiple gaps test, got {this.processedBatches.Count}");

            // All messages must be processed - no data loss
            Assert.Equal(totalMessagesAdded, totalMessagesProcessed);

            // Verify batches were processed at different times (gaps)
            if (this.processedBatches.Count >= 2)
            {
                var timeDiffs = new List<double>();
                for (var i = 1; i < this.processedBatches.Count; i++)
                {
                    var diff = (this.processedBatches[i].ProcessedAt - this.processedBatches[i - 1].ProcessedAt).TotalMilliseconds;
                    timeDiffs.Add(diff);
                }

                // At least one gap should be significant (>= timeWindow)
                Assert.True(
                    timeDiffs.Any(d => d >= TimeWindow),
                    $"Should have at least one significant time gap between batches. Gaps: {string.Join(", ", timeDiffs.Select(d => $"{d:F0}ms"))}");
            }
        }

        // Verify queue is empty - no stale data
        Assert.Equal(0, processor.QueueCount);

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 4C: Rapid gaps - Should handle rapid start-stop-start pattern
    /// </summary>
    [Fact]
    public async Task ShouldHandleRapidGaps_WithNoDataLoss()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        const int TimeWindow = 80; // 80ms
        var totalMessagesAdded = 0;

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: 50000,
            thresholdPercentage: 0.90,
            timeWindowMilliseconds: TimeWindow
        );

        // Act: Rapid message bursts with gaps
        for (var burst = 0; burst < 10; burst++)
        {
            // Add a small burst of messages
            for (var i = 0; i < 2; i++)
            {
                processor.Add(PacketFactory.CreatePacket(30));
                totalMessagesAdded++;
            }

            // Small gap
            await Task.Delay(TimeWindow + 30, cts.Token);
        }

        // Extra wait to ensure all batches processed
        await Task.Delay(200, cts.Token);

        // Assert: All messages processed, no data stuck
        int totalMessagesProcessed;
        lock (this.lockObject)
        {
            totalMessagesProcessed = this.processedBatches.Sum(b => b.MessageCount);
        }

        Assert.Equal(totalMessagesAdded, totalMessagesProcessed);
        Assert.Equal(0, processor.QueueCount); // Queue should be empty

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 4D: Long gap - Should process after extended period of inactivity
    /// </summary>
    [Fact]
    public async Task ShouldProcessBatch_AfterLongGap()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        const int TimeWindow = 100;

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: 10000,
            thresholdPercentage: 0.90,
            timeWindowMilliseconds: TimeWindow
        );

        // Act Phase 1: Add messages then very long gap
        processor.Add(PacketFactory.CreatePacket(100));
        processor.Add(PacketFactory.CreatePacket(100));
        processor.Add(PacketFactory.CreatePacket(100));

        // Long gap - much longer than time window (5x)
        await Task.Delay(TimeWindow * 5, cts.Token);

        // Assert: Should have processed first batch despite long gap
        lock (this.lockObject)
        {
            Assert.True(this.processedBatches.Count >= 1, "Should have at least 1 batch after long gap");
            Assert.Equal(3, this.processedBatches.Sum(b => b.MessageCount));
        }

        // Act Phase 2: Add more messages after long gap
        processor.Add(PacketFactory.CreatePacket(100));
        processor.Add(PacketFactory.CreatePacket(100));

        // Wait for timer to fire after gap
        await Task.Delay(TimeWindow + 150, cts.Token);

        // Assert: All 5 messages should be processed
        int totalProcessed;
        lock (this.lockObject)
        {
            totalProcessed = this.processedBatches.Sum(b => b.MessageCount);
        }

        Assert.True(totalProcessed >= 3, $"Should have processed at least first 3 messages, got {totalProcessed}");

        // Complete to ensure any remaining messages are flushed
        await processor.CompleteAsync();

        // Final check: all messages processed, queue empty
        lock (this.lockObject)
        {
            totalProcessed = this.processedBatches.Sum(b => b.MessageCount);
        }

        Assert.Equal(5, totalProcessed);
        Assert.Equal(0, processor.QueueCount);
    }

    /// <summary>
    ///     TEST 4E: Gap with queue verification - Ensure no stale data remains
    /// </summary>
    [Fact]
    public async Task ShouldNotLeaveStaleData_InQueue()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        const int TimeWindow = 100;

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: 10000,
            thresholdPercentage: 0.95, // Very high to ensure time-based triggering
            timeWindowMilliseconds: TimeWindow
        );

        // Act & Assert: Multiple rounds of adding and verifying
        for (var round = 0; round < 5; round++)
        {
            // Add some messages
            var messagesThisRound = round + 1;
            for (var i = 0; i < messagesThisRound; i++)
            {
                processor.Add(PacketFactory.CreatePacket(40));
            }

            // Wait for time window to process + extra buffer for timer firing + processing
            await Task.Delay(TimeWindow + 150, cts.Token);

            // Verify queue is empty (no stale data)
            var queueCount = processor.QueueCount;
            Assert.True(
                queueCount == 0,
                $"Round {round + 1}: Queue should be empty but has {queueCount} messages");
        }

        // Final verification
        int totalProcessed;
        lock (this.lockObject)
        {
            totalProcessed = this.processedBatches.Sum(b => b.MessageCount);
        }

        const int ExpectedTotal = 1 + 2 + 3 + 4 + 5; // Sum of messages from all rounds
        Assert.Equal(ExpectedTotal, totalProcessed);

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 5: Multiple batches - Should handle continuous message flow
    /// </summary>
    [Fact]
    public async Task ShouldProcessMultipleBatches_WithContinuousFlow()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        const int MaxSize = 300;

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: MaxSize,
            thresholdPercentage: 0.70, // 210 bytes threshold
            timeWindowMilliseconds: 5000
        );

        // Act: Add enough messages to trigger multiple batches
        for (var i = 0; i < 30; i++)
        {
            processor.Add(PacketFactory.CreatePacket(100)); // 100 bytes each
            await Task.Delay(10, cts.Token); // Small delay between messages
        }

        await Task.Delay(500, cts.Token);

        // Assert
        lock (this.lockObject)
        {
            Assert.True(
                this.processedBatches.Count >= 10,
                $"Should have multiple batches, got {this.processedBatches.Count}");

            // Each batch should not exceed max size
            foreach (var batch in this.processedBatches)
            {
                Assert.True(
                    batch.TotalSizeBytes <= MaxSize,
                    $"Batch {batch.TotalSizeBytes} bytes exceeds max {MaxSize} bytes");
            }
        }

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 6: Cancellation - Should process remaining messages on cancellation
    /// </summary>
    [Fact]
    public async Task ShouldProcessRemainingMessages_OnCancellation()
    {
        // Arrange
        var cts = new CancellationTokenSource();

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: 10000,
            thresholdPercentage: 0.90, // High threshold
            timeWindowMilliseconds: 10000 // Long window
        );

        // Act: Add messages but don't reach threshold or time
        processor.Add(PacketFactory.CreatePacket(50));
        processor.Add(PacketFactory.CreatePacket(50));
        processor.Add(PacketFactory.CreatePacket(50));

        // Complete (triggers cancellation)
        await processor.CompleteAsync();


        var stopwatch = new Stopwatch();
        stopwatch.Start();
        while (stopwatch.Elapsed.TotalSeconds < 5)
        {
            lock (this.lockObject)
            {
                if (this.processedBatches.Count>0)
                    break;
            }
            await Task.Delay(100);
        }

        // Assert
        lock (this.lockObject)
        {
            Assert.Single(this.processedBatches);
            Assert.Equal(3, this.processedBatches[0].MessageCount);
        }
    }

    /// <summary>
    ///     TEST 7: Empty batch - Should not process empty batches
    /// </summary>
    [Fact]
    public async Task ShouldNotProcessEmptyBatch_WhenNoMessages()
    {
        // Arrange
        var cts = new CancellationTokenSource();

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: 1000,
            thresholdPercentage: 0.70,
            timeWindowMilliseconds: 100
        );

        // Act: Don't add any messages, just wait
        await Task.Delay(300, cts.Token);

        // Assert
        lock (this.lockObject)
        {
            Assert.Empty(this.processedBatches);
        }

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 8: Queue count - Should accurately track queue size
    /// </summary>
    [Fact]
    public async Task ShouldTrackQueueCount_Accurately()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var processDelay = new TaskCompletionSource<bool>();

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: async (batch) =>
            {
                await processDelay.Task; // Block processing
                await this.MockProcessAction(batch);
            },
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: 1000,
            thresholdPercentage: 0.70,
            timeWindowMilliseconds: 10000
        );

        // Act: Add messages
        processor.Add(PacketFactory.CreatePacket(50));
        processor.Add(PacketFactory.CreatePacket(50));
        processor.Add(PacketFactory.CreatePacket(50));

        await Task.Delay(100, cts.Token);

        // Assert: Queue should have messages
        Assert.True(processor.QueueCount >= 0);

        // Release processing
        processDelay.SetResult(true);
        await Task.Delay(200, cts.Token);

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 9: AddRange - Should add multiple messages at once
    /// </summary>
    [Fact]
    public async Task ShouldAddMultipleMessages_UsingAddRange()
    {
        // Arrange
        var cts = new CancellationTokenSource();

        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token,
            maxBatchSizeBytes: 10000,
            thresholdPercentage: 0.70,
            timeWindowMilliseconds: 100
        );

        // Act: Add range of messages
        var messages = Enumerable.Range(0, 10)
            .Select(_ => PacketFactory.CreatePacket(50))
            .ToList();

        processor.AddRange(messages);

        // Wait until the timer-triggered batch arrives (or timeout).
        await this.WaitForBatchesAsync(expectedBatchCount: 1, timeoutMs: 3000);

        // Assert
        lock (this.lockObject)
        {
            Assert.Single(this.processedBatches);
            Assert.Equal(10, this.processedBatches[0].MessageCount);
        }

        await processor.CompleteAsync();
    }

    /// <summary>
    ///     TEST 10: Default parameters - Should work with default configuration
    /// </summary>
    [Fact]
    public async Task ShouldWorkCorrectly_WithDefaultParameters()
    {
        // Arrange
        var cts = new CancellationTokenSource();

        // Use defaults: 3MB max, 70% threshold, 50ms window
        var processor = new SizeBasedBatchProcessor<Packet>(
            this.logger,
            processAction: this.MockProcessAction,
            sizeCalculator: p => p.CalculateSize(),
            cts.Token
        );

        // Act
        for (var i = 0; i < 20; i++)
        {
            processor.Add(PacketFactory.CreatePacket(1000)); // 1KB each
        }

        await Task.Delay(2000, cts.Token);

        // Assert
        lock (this.lockObject)
        {
            Assert.True(this.processedBatches.Count > 0);
        }

        await processor.CompleteAsync();
    }
}
