// <copyright file="INotificationStreamWriterService.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Proto.Core.Abstractions;

/// <summary>
/// Service for managing notification stream writers and sending messages to connected clients.
/// </summary>
/// <typeparam name="TNotificationMessage">The type of notification message to stream.</typeparam>
public interface INotificationStreamWriterService<TNotificationMessage>
{
    /// <summary>
    /// Adds a stream writer for the specified data source and holds the connection open until cancellation is requested.
    /// </summary>
    /// <param name="dataSource">The data source identifier.</param>
    /// <param name="notificationStreamWriter">The server stream writer to add.</param>
    /// <param name="cancellationToken">A cancellation token to signal when the stream should be released.</param>
    /// <returns>A task that completes when the stream writer is released.</returns>
    Task AddAndHoldStreamWriter(string dataSource, IServerStreamWriter<TNotificationMessage> notificationStreamWriter, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a notification message to all connected stream writers for the specified data source.
    /// </summary>
    /// <param name="dataSource">The data source identifier.</param>
    /// <param name="message">The notification message to send.</param>
    /// <returns>A task representing the asynchronous send operation.</returns>
    Task SendMessage(string dataSource, TNotificationMessage message);
}
