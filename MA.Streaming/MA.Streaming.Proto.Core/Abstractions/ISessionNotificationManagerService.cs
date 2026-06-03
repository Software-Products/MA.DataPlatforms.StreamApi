// <copyright file="ISessionNotificationManagerService.cs" company="Motion Applied Ltd.">
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

using MA.Streaming.API;

namespace MA.Streaming.Proto.Core.Abstractions;

/// <summary>
/// Manages session start and stop notification streams for data sources.
/// </summary>
public interface ISessionNotificationManagerService
{
    /// <summary>
    /// Starts the session notification manager service.
    /// </summary>
    void Start();

    /// <summary>
    /// Adds a stream writer for session start notifications.
    /// </summary>
    /// <param name="dataSource">The data source to monitor for session start events.</param>
    /// <param name="notificationStreamWriter">The server stream writer to send start notifications to.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task AddStartNotificationStream(string dataSource, IServerStreamWriter<GetSessionStartNotificationResponse> notificationStreamWriter, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a stream writer for session stop notifications.
    /// </summary>
    /// <param name="dataSource">The data source to monitor for session stop events.</param>
    /// <param name="notificationStreamWriter">The server stream writer to send stop notifications to.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task AddStopNotificationStream(string dataSource, IServerStreamWriter<GetSessionStopNotificationResponse> notificationStreamWriter, CancellationToken cancellationToken = default);
}
