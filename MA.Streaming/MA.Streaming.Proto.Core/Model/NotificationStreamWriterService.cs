// <copyright file="NotificationStreamWriterService.cs" company="Motion Applied Ltd.">
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

using Grpc.Core;

using MA.Streaming.Core;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Model;

/// <summary>
/// Service for managing notification stream writers and sending messages to connected clients.
/// </summary>
/// <typeparam name="TNotificationMessage">The type of notification message to stream.</typeparam>
public class NotificationStreamWriterService<TNotificationMessage> : INotificationStreamWriterService<TNotificationMessage>
{
    private static readonly ThreadSafeInMemoryRepository<string, List<StreamWriterItem<TNotificationMessage>>> WritersRepository = new();

    /// <inheritdoc />
    public async Task AddAndHoldStreamWriter(string dataSource, IServerStreamWriter<TNotificationMessage> notificationStreamWriter, CancellationToken cancellationToken = default)
    {
        var streamWriterItems = WritersRepository.Get(dataSource);
        if (streamWriterItems == null)
        {
            streamWriterItems = [];
            WritersRepository.AddOrUpdate(dataSource, streamWriterItems);
        }

        var autoResetEvent = new AutoResetEvent(false);
        streamWriterItems.Add(new StreamWriterItem<TNotificationMessage>(notificationStreamWriter, autoResetEvent));
        await using (cancellationToken.Register(() => autoResetEvent.Set()))
        {
            autoResetEvent.WaitOne();
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task SendMessage(string dataSource, TNotificationMessage message)
    {
        var streamWriterItems = WritersRepository.Get(dataSource);
        if (streamWriterItems == null)
        {
            return;
        }

        var newStreamWriterItems = new List<StreamWriterItem<TNotificationMessage>>();
        foreach (var streamWriterItem in streamWriterItems)
        {
            try
            {
                await streamWriterItem.StreamWriter.WriteAsync(message);
                newStreamWriterItems.Add(streamWriterItem);
            }
            catch
            {
                streamWriterItem.AutoResetEvent.Set();
            }
        }

        WritersRepository.AddOrUpdate(dataSource, newStreamWriterItems);
    }
}
