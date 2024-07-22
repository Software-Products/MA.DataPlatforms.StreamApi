// <copyright file="AddAssociateSessionRequestHandler.cs" company="McLaren Applied Ltd.">
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

using Google.Protobuf;

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core.SessionManagement;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Handlers;

public class AddAssociateSessionRequestHandler : IAddAssociateSessionRequestHandler
{
    private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository;
    private readonly ILogger logger;
    private readonly IPacketWriterHelper packetWriterHelper;
    private readonly ITypeNameProvider typeNameProvider;

    public AddAssociateSessionRequestHandler(
        IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository,
        ILogger logger,
        IPacketWriterHelper packetWriterHelper,
        ITypeNameProvider typeNameProvider)
    {
        this.sessionInfoRepository = sessionInfoRepository;
        this.logger = logger;
        this.packetWriterHelper = packetWriterHelper;
        this.typeNameProvider = typeNameProvider;
    }

    public async Task<AddAssociateSessionResponse> AddAssociateSession(AddAssociateSessionRequest request)
    {
        var foundSessionDetail = this.sessionInfoRepository.Get(request.SessionKey);
        if (foundSessionDetail == null)
        {
            return this.CreateUnsuccessfulResponse(request);
        }

        var sessionInfo = CreateSessionInfoPacket(request, foundSessionDetail);

        this.WriteInfoPacket(request, sessionInfo, foundSessionDetail);

        return await Task.FromResult(CreateSuccessfulResponse());
    }

    private static AddAssociateSessionResponse CreateSuccessfulResponse()
    {
        return new AddAssociateSessionResponse
        {
            Success = true
        };
    }

    private void WriteInfoPacket(AddAssociateSessionRequest request, SessionInfoPacket sessionInfo, SessionDetailRecord foundSessionDetail)
    {
        this.packetWriterHelper.WriteInfoPacket(
            request.SessionKey,
            this.typeNameProvider.SessionInfoPacketTypeName,
            sessionInfo.ToByteString(),
            foundSessionDetail.DataSource);
    }

    private static SessionInfoPacket CreateSessionInfoPacket(AddAssociateSessionRequest request, SessionDetailRecord foundSessionDetail)
    {
        var sessionInfoAssociatedIds = new List<string>(foundSessionDetail.SessionInfoPacket.AssociatedKeys)
        {
            request.AssociateSessionKey
        };

        var sessionInfo = new SessionInfoPacket
        {
            Type = foundSessionDetail.SessionInfoPacket.Type,
            AssociateSessionKeys =
            {
                sessionInfoAssociatedIds
            },
            DataSource = foundSessionDetail.DataSource,
            Identifier = foundSessionDetail.SessionInfoPacket.Identifier,
            Version = foundSessionDetail.SessionInfoPacket.Version
        };
        return sessionInfo;
    }

    private AddAssociateSessionResponse CreateUnsuccessfulResponse(AddAssociateSessionRequest request)
    {
        this.logger.Error(
            $"try to add associatedId to an identifier for a session which is not added in the session management service. session key:{request.SessionKey}");
        return new AddAssociateSessionResponse
        {
            Success = false
        };
    }
}
