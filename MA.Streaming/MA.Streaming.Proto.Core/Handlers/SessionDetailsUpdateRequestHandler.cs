// <copyright file="SessionDetailsUpdateRequestHandler.cs" company="McLaren Applied Ltd.">
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

public class SessionDetailsUpdateRequestHandler : ISessionDetailsUpdateRequestHandler
{
    private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository;
    private readonly ILogger logger;
    private readonly IPacketWriterHelper packetWriterHelper;
    private readonly ITypeNameProvider typeNameProvider;

    public SessionDetailsUpdateRequestHandler(
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

    public async Task<UpdateSessionDetailsResponse> UpdateSessionDetails(UpdateSessionDetailsRequest request)
    {
        var foundSessionDetail = this.sessionInfoRepository.Get(request.SessionKey);
        if (foundSessionDetail == null)
        {
            return this.CreateUnsuccessfulResponse(request);
        }

        var sessionInfo = CreatePacket(request, foundSessionDetail);
        this.WritePacket(request, sessionInfo, foundSessionDetail);

        return await Task.FromResult(CreateSuccessfulMessage());
    }

    private static UpdateSessionDetailsResponse CreateSuccessfulMessage()
    {
        return new UpdateSessionDetailsResponse
        {
            Success = true
        };
    }

    private UpdateSessionDetailsResponse CreateUnsuccessfulResponse(UpdateSessionDetailsRequest request)
    {
        this.logger.Error(
            $"Unable to update session details for a session which is not currently registered with the session management service. session key: {request.SessionKey}");
        return new UpdateSessionDetailsResponse
        {
            Success = false
        };
    }

    private void WritePacket(UpdateSessionDetailsRequest request, SessionInfoPacket sessionInfo, SessionDetailRecord foundSessionDetail)
    {
        this.packetWriterHelper.WriteInfoPacket(
            request.SessionKey,
            this.typeNameProvider.SessionInfoPacketTypeName,
            sessionInfo.ToByteString(),
            foundSessionDetail.DataSource);
    }

    private static SessionInfoPacket CreatePacket(UpdateSessionDetailsRequest request, SessionDetailRecord foundSessionDetail)
    {
        var sessionInfo = new SessionInfoPacket
        {
            Type = foundSessionDetail.SessionInfoPacket.Type,
            AssociateSessionKeys =
            {
                foundSessionDetail.SessionInfoPacket.AssociatedKeys
            },
            DataSource = foundSessionDetail.DataSource,
            Identifier = foundSessionDetail.SessionInfoPacket.Identifier,
            Version = foundSessionDetail.SessionInfoPacket.Version,
        };
        foreach (var foundDetail in foundSessionDetail.SessionInfoPacket.Details)
        {
            sessionInfo.Details[foundDetail.Key] = foundDetail.Value;
        }

        foreach (var newDetail in request.Details)
        {
            sessionInfo.Details[newDetail.Key] = newDetail.Value;
        }

        return sessionInfo;
    }
}
