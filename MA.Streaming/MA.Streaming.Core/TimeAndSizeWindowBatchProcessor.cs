// <copyright file="TimeAndSizeWindowBatchProcessor.cs" company="McLaren Applied Ltd.">
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

using System.Collections.Concurrent;
using System.Timers;

using Timer = System.Timers.Timer;

namespace MA.Streaming.Core;

public class TimeAndSizeWindowBatchProcessor<T>
{
    private readonly BlockingCollection<T> messageQueue = [];
    private readonly int batchSize;
    private readonly Func<IReadOnlyList<T>, Task> processAction;
    private readonly TimeSpan timeWindowTimeSpan;
    private readonly Timer timer = new(1);
    private readonly AutoResetEvent handleDelayAutoResetEvent = new(false);

    public TimeAndSizeWindowBatchProcessor(
        Func<IReadOnlyList<T>, Task> processAction,
        CancellationTokenSource cancellationTokenSource,
        int batchSize = 15000,
        int timeWindowSize = 1)
    {
        this.batchSize = batchSize;
        this.processAction = processAction;
        this.timeWindowTimeSpan = TimeSpan.FromMilliseconds(timeWindowSize);
        _ = Task.Run(() => this.ProcessMessagesAsync(cancellationTokenSource.Token));
        this.timer.Elapsed += this.Timer_Elapsed;
    }

    public void Add(T item)
    {
        this.messageQueue.Add(item);
    }

    private void Timer_Elapsed(object? sender, ElapsedEventArgs e)
    {
        this.timer.Enabled = false;
        this.handleDelayAutoResetEvent.Set();
    }

    private async Task ProcessMessagesAsync(CancellationToken token)
    {
        var messages = new List<T>();
        var nextBatchTime = DateTime.Now.Add(this.timeWindowTimeSpan);

        while (!token.IsCancellationRequested)
        {
            while (this.messageQueue.TryTake(out var message))
            {
                messages.Add(message);

                if (messages.Count < this.batchSize &&
                    DateTime.Now < nextBatchTime)
                {
                    continue;
                }

                await this.ProcessBatchAsync(messages);
                messages.Clear();
                nextBatchTime = DateTime.Now.Add(this.timeWindowTimeSpan);
            }

            if (messages is [_, ..] &&
                DateTime.Now >= nextBatchTime)
            {
                await this.ProcessBatchAsync(messages);
                messages.Clear();
                nextBatchTime = DateTime.Now.Add(this.timeWindowTimeSpan);
            }

            await Task.Delay(1, token);
        }

        if (messages.Count > 0)
        {
            await this.ProcessBatchAsync(messages);
        }
    }

    private async Task ProcessBatchAsync(IReadOnlyList<T> data)
    {
        await this.processAction(data);
    }
}
