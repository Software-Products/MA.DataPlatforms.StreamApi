// <copyright file="GenericBuffer.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing;

namespace MA.Streaming.Core;

public class GenericBuffer<T>
{
    private readonly IBufferConsumer<T> bufferConsumer;
    private readonly BlockingCollection<T> buffer = [];
    private readonly CancellationToken token;
    private readonly SemaphoreSlim semaphore = new(0);

    public GenericBuffer(IBufferConsumer<T> bufferConsumer, ICancellationTokenSourceProvider? cancellationTokenSourceProvider = null)
    {
        this.bufferConsumer = bufferConsumer;

        this.token = (cancellationTokenSourceProvider ?? new CancellationTokenSourceProvider()).Provide().Token;
        this.StartProcessing();
    }

    public void AddData(T data)
    {
        this.buffer.Add(data, this.token);
        this.semaphore.Release();
    }

    private void StartProcessing()
    {
        Task.Run(
            async () =>
            {
                try
                {
                    while (!this.token.IsCancellationRequested)
                    {
                        await this.semaphore.WaitAsync(this.token);
                        if (this.buffer.TryTake(out var data))
                        {
                            await this.bufferConsumer.ConsumeAsync(data);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Processing was canceled.");
                }
            },
            this.token);
    }
}
