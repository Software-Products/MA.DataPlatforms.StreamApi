// <copyright file="SessionNotificationManagerService.cs" company="Motion Applied Ltd.">
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

using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Model;

/// <inheritdoc/>
public class SessionNotificationManagerService : ISessionNotificationManagerService
{
    private readonly ISessionInfoService sessionInfoService;
    private readonly INotificationStreamWriterService<GetSessionStartNotificationResponse> startNotificationStreamWriterService;
    private readonly INotificationStreamWriterService<GetSessionStopNotificationResponse> stopNotificationStreamWriterService;

    /// <summary>
    /// Initializes a new instance of the <see cref="SessionNotificationManagerService"/> class.
    /// </summary>
    /// <param name="sessionInfoService">The session info service to subscribe to session events.</param>
    /// <param name="startNotificationStreamWriterService">The service for managing session start notification streams.</param>
    /// <param name="stopNotificationStreamWriterService">The service for managing session stop notification streams.</param>
    public SessionNotificationManagerService(
        ISessionInfoService sessionInfoService,
        INotificationStreamWriterService<GetSessionStartNotificationResponse> startNotificationStreamWriterService,
        INotificationStreamWriterService<GetSessionStopNotificationResponse> stopNotificationStreamWriterService)
    {
        this.sessionInfoService = sessionInfoService;
        this.startNotificationStreamWriterService = startNotificationStreamWriterService;
        this.stopNotificationStreamWriterService = stopNotificationStreamWriterService;
    }

    /// <summary>
    /// Gets a value indicating whether the service has been started.
    /// </summary>
    public bool Started { get; private set; }

    /// <inheritdoc/>
    public async Task AddStartNotificationStream(string dataSource, IServerStreamWriter<GetSessionStartNotificationResponse> notificationStreamWriter, CancellationToken cancellationToken = default)
    {
        await this.startNotificationStreamWriterService.AddAndHoldStreamWriter(dataSource, notificationStreamWriter, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task AddStopNotificationStream(string dataSource, IServerStreamWriter<GetSessionStopNotificationResponse> notificationStreamWriter, CancellationToken cancellationToken = default)
    {
        await this.stopNotificationStreamWriterService.AddAndHoldStreamWriter(dataSource, notificationStreamWriter, cancellationToken);
    }

    /// <inheritdoc/>
    public void Start()
    {
        if (this.Started)
        {
            return;
        }

        this.sessionInfoService.SessionStarted += this.SessionInfoService_SessionStarted;
        this.sessionInfoService.SessionStopped += this.SessionInfoService_SessionStopped;
        this.Started = true;
    }

    private void SessionInfoService_SessionStarted(object? sender, SessionsInfoChangeEventArg e)
    {
        var getSessionStartNotificationResponse = new GetSessionStartNotificationResponse
        {
            DataSource = e.DataSource,
            SessionKey = e.SessionKey
        };
        this.startNotificationStreamWriterService.SendMessage(e.DataSource, getSessionStartNotificationResponse).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    private void SessionInfoService_SessionStopped(object? sender, SessionsInfoChangeEventArg e)
    {
        var getSessionStopNotificationResponse = new GetSessionStopNotificationResponse
        {
            DataSource = e.DataSource,
            SessionKey = e.SessionKey
        };
        this.stopNotificationStreamWriterService.SendMessage(e.DataSource, getSessionStopNotificationResponse).ConfigureAwait(false).GetAwaiter().GetResult();
    }
}
