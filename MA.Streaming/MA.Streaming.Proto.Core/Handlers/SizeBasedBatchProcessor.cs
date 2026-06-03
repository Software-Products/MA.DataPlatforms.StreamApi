// <copyright file="SizeBasedBatchProcessor.cs" company="Motion Applied Ltd.">
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
using System.Timers;

using MA.Common.Abstractions;

using Timer = System.Timers.Timer;

namespace MA.Streaming.Proto.Core.Handlers;

public class SizeBasedBatchProcessor<T>
{
    private readonly BlockingCollection<T> messageQueue = new();
    private readonly ILogger logger;
    private readonly Func<IReadOnlyList<T>, Task> processAction;
    private readonly Func<T, int> sizeCalculator;
    private readonly int maxBatchSizeBytes;
    private readonly int batchSizeThresholdBytes;
    private readonly Timer timer;
    private readonly CancellationToken cancellationToken;
    private readonly AutoResetEvent timerSignal = new(false);
    private bool completed;

    private const int DefaultMaxBatchSizeBytes = 3 * 1024 * 1024;
    private const double DefaultThresholdPercentage = 0.70;

    public SizeBasedBatchProcessor(
        ILogger logger,
        Func<IReadOnlyList<T>, Task> processAction,
        Func<T, int> sizeCalculator,
        CancellationToken cancellationToken,
        int maxBatchSizeBytes = DefaultMaxBatchSizeBytes,
        double thresholdPercentage = DefaultThresholdPercentage,
        int timeWindowMilliseconds = 50)
    {
        this.cancellationToken = cancellationToken;
        if (thresholdPercentage is <= 0 or > 1)
        {
            throw new ArgumentException("Threshold percentage must be between 0 and 1", nameof(thresholdPercentage));
        }

        this.logger = logger;
        this.processAction = processAction ?? throw new ArgumentNullException(nameof(processAction));
        this.sizeCalculator = sizeCalculator ?? throw new ArgumentNullException(nameof(sizeCalculator));
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        this.batchSizeThresholdBytes = (int)(maxBatchSizeBytes * thresholdPercentage);

        this.timer = new Timer(timeWindowMilliseconds);
        this.timer.AutoReset = false;
        this.timer.Elapsed += this.Timer_Elapsed;

        _ = Task.Run(this.ProcessMessagesAsync);
    }

    public void Add(T item)
    {
        if (EqualityComparer<T>.Default.Equals(item, default))
        {
            throw new ArgumentNullException(nameof(item));
        }

        this.messageQueue.Add(item);
    }

    public void AddRange(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            this.messageQueue.Add(item);
        }
    }

    private void Timer_Elapsed(object? sender, ElapsedEventArgs e)
    {
        this.timerSignal.Set();
    }

    private async Task ProcessMessagesAsync()
    {
        var messages = new List<T>();
        var currentBatchSizeBytes = 0;
        var isTimerRunning = false;

        while (!this.cancellationToken.IsCancellationRequested && !this.completed)
        {
            var hasMessage = this.messageQueue.TryTake(out var message, 100);

            if (hasMessage && !EqualityComparer<T>.Default.Equals(message, default))
            {
                var (newBatchSize, timerRunning) = await this.ProcessMessageAsync(
                    message!,
                    messages,
                    currentBatchSizeBytes,
                    isTimerRunning);

                currentBatchSizeBytes = newBatchSize;
                isTimerRunning = timerRunning;

                if (currentBatchSizeBytes < this.batchSizeThresholdBytes)
                {
                    continue;
                }
            }
            else if (!this.ShouldProcessBatchOnTimer(messages.Count))
            {
                continue;
            }

            await this.ProcessBatchAsync(messages, currentBatchSizeBytes);
            messages.Clear();
            currentBatchSizeBytes = 0;
            this.timer.Stop();
            isTimerRunning = false;
        }

        // Process remaining messages on cancellation
        if (messages.Count > 0)
        {
            await this.ProcessBatchAsync(messages, currentBatchSizeBytes);
        }

        this.timer.Stop();
        this.timer.Dispose();
        this.timerSignal.Dispose();
    }

    private async Task<(int batchSize, bool timerRunning)> ProcessMessageAsync(
        T message,
        List<T> messages,
        int currentBatchSizeBytes,
        bool isTimerRunning)
    {
        var messageSize = this.sizeCalculator(message);

        if (currentBatchSizeBytes + messageSize > this.maxBatchSizeBytes &&
            messages.Count > 0)
        {
            await this.ProcessBatchAsync(messages, currentBatchSizeBytes);
            messages.Clear();
            currentBatchSizeBytes = 0;
            this.timer.Stop();
            this.timer.Start();
            isTimerRunning = true;
        }

        messages.Add(message);
        currentBatchSizeBytes += messageSize;

        if (isTimerRunning)
        {
            return (currentBatchSizeBytes, isTimerRunning);
        }

        this.timer.Start();
        isTimerRunning = true;

        return (currentBatchSizeBytes, isTimerRunning);
    }

    private bool ShouldProcessBatchOnTimer(int messageCount)
    {
        return this.timerSignal.WaitOne(0) && messageCount > 0;
    }

    private async Task ProcessBatchAsync(List<T> data, int batchSizeBytes)
    {
        if (data.Count == 0)
        {
            return;
        }

        try
        {
            this.logger.Trace($"Processing batch: {data.Count} items, {batchSizeBytes:N0} bytes ({(double)batchSizeBytes / this.maxBatchSizeBytes * 100:F1}% of max)");
            await this.processAction(data);
        }
        catch (Exception ex)
        {
            this.logger.Warning($"Error processing batch: {ex.Message}");
            throw;
        }
    }

    public async Task CompleteAsync()
    {
        this.messageQueue.CompleteAdding();

        while (this.messageQueue.Count > 0)
        {
            await Task.Delay(100);
        }

        this.completed = true;
    }

    public int QueueCount => this.messageQueue.Count;
}
