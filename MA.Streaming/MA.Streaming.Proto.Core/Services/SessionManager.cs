// <copyright file="SessionManager.cs" company="McLaren Applied Ltd.">
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

using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core.SessionManagement;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Services;

public sealed class SessionManager : SessionManagementService.SessionManagementServiceBase
{
    private readonly ISessionCreationRequestHandler sessionCreationRequestHandler;
    private readonly ISessionEndingRequestHandler sessionEndingRequestHandler;
    private readonly IGetSessionInfoRequestHandler getSessionInfoRequestHandler;
    private readonly ISessionIdentifierUpdateRequestHandler sessionIdentifierUpdateRequestHandler;
    private readonly IAddAssociateSessionRequestHandler requestHandler;
    private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository;
    private readonly ISessionNotificationManagerService sessionNotificationManagerService;

    private readonly bool enabled;

    public SessionManager(
        ISessionCreationRequestHandler sessionCreationRequestHandler,
        ISessionEndingRequestHandler sessionEndingRequestHandler,
        IGetSessionInfoRequestHandler getSessionInfoRequestHandler,
        ISessionIdentifierUpdateRequestHandler sessionIdentifierUpdateRequestHandler,
        IAddAssociateSessionRequestHandler requestHandler,
        IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository,
        IStreamingApiConfigurationProvider apiConfigurationProvider,
        ISessionNotificationManagerService sessionNotificationManagerService)
    {
        this.sessionCreationRequestHandler = sessionCreationRequestHandler;
        this.sessionEndingRequestHandler = sessionEndingRequestHandler;
        this.getSessionInfoRequestHandler = getSessionInfoRequestHandler;
        this.sessionIdentifierUpdateRequestHandler = sessionIdentifierUpdateRequestHandler;
        this.requestHandler = requestHandler;
        this.sessionInfoRepository = sessionInfoRepository;
        this.sessionNotificationManagerService = sessionNotificationManagerService;

        this.enabled = apiConfigurationProvider.Provide().IntegrateSessionManagement;
    }

    public override async Task<CreateSessionResponse> CreateSession(CreateSessionRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new CreateSessionResponse();
        }

        return await this.sessionCreationRequestHandler.Handle(request);
    }

    public override async Task<EndSessionResponse> EndSession(EndSessionRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new EndSessionResponse();
        }

        return await this.sessionEndingRequestHandler.Handle(request);
    }

    public override async Task<GetCurrentSessionsResponse> GetCurrentSessions(GetCurrentSessionsRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new GetCurrentSessionsResponse();
        }

        var allSession = this.sessionInfoRepository.GetAll().Where(i => i.DataSource == request.DataSource);
        return await Task.FromResult(
            new GetCurrentSessionsResponse
            {
                SessionKeys =
                {
                    allSession.Select(i => i.SessionKey)
                }
            });
    }

    public override async Task<GetSessionInfoResponse> GetSessionInfo(GetSessionInfoRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new GetSessionInfoResponse();
        }

        return await this.getSessionInfoRequestHandler.GetSessionInfo(request);
    }

    public override async Task GetSessionStartNotification(
        GetSessionStartNotificationRequest request,
        IServerStreamWriter<GetSessionStartNotificationResponse> responseStream,
        ServerCallContext context)
    {
        if (!this.enabled)
        {
            return;
        }

        await this.sessionNotificationManagerService.AddStartNotificationStream(request.DataSource, responseStream);
    }

    public override async Task GetSessionStopNotification(
        GetSessionStopNotificationRequest request,
        IServerStreamWriter<GetSessionStopNotificationResponse> responseStream,
        ServerCallContext context)
    {
        if (!this.enabled)
        {
            return;
        }

        await this.sessionNotificationManagerService.AddStopNotificationStream(request.DataSource, responseStream);
    }

    public override async Task<UpdateSessionIdentifierResponse> UpdateSessionIdentifier(UpdateSessionIdentifierRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new UpdateSessionIdentifierResponse();
        }

        return await this.sessionIdentifierUpdateRequestHandler.UpdateSessionIdentifier(request);
    }

    public override async Task<AddAssociateSessionResponse> AddAssociateSession(AddAssociateSessionRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new AddAssociateSessionResponse();
        }

        return await this.requestHandler.AddAssociateSession(request);
    }
}
