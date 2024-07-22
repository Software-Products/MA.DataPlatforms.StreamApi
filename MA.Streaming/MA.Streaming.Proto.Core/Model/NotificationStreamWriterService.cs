// <copyright file="NotificationStreamWriterService.cs" company="McLaren Applied Ltd.">
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

using Grpc.Core;

using MA.Streaming.Core;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Model;

public class NotificationStreamWriterService<TNotificationMessage> : INotificationStreamWriterService<TNotificationMessage>
{
    private static readonly ThreadSafeInMemoryRepository<string, List<StreamWriterItem<TNotificationMessage>>> WritersRepository = new();

    public async Task AddAndHoldStreamWriter(string dataSource, IServerStreamWriter<TNotificationMessage> notificationStreamWriter)
    {
        var streamWriterItems = WritersRepository.Get(dataSource);
        if (streamWriterItems == null)
        {
            streamWriterItems = new List<StreamWriterItem<TNotificationMessage>>();
            WritersRepository.AddOrUpdate(dataSource, streamWriterItems);
        }

        var autoResetEvent = new AutoResetEvent(false);
        streamWriterItems.Add(new StreamWriterItem<TNotificationMessage>(notificationStreamWriter, autoResetEvent));
        autoResetEvent.WaitOne();
        await Task.CompletedTask;
    }

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
