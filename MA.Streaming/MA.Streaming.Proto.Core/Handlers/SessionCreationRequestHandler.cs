// <copyright file="SessionCreationRequestHandler.cs" company="Motion Applied Ltd.">
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

using Google.Protobuf;
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Handlers;

public class SessionCreationRequestHandler : ISessionCreationRequestHandler
{
    private readonly IKeyGeneratorService keyGeneratorService;
    private readonly IPacketWriterHelper packetWriterHelper;
    private readonly ISessionInfoService sessionInfoService;
    private readonly IKafkaBrokerRoutesOffsetExtractor kafkaBrokerRoutesOffsetExtractor;
    private readonly ITypeNameProvider typeNameProvider;
    public SessionCreationRequestHandler(
        IKafkaBrokerRoutesOffsetExtractor kafkaBrokerRoutesOffsetExtractor,
        ITypeNameProvider typeNameProvider,
        IKeyGeneratorService keyGeneratorService,
        IPacketWriterHelper packetWriterHelper,
        ISessionInfoService sessionInfoService)
    {
        this.keyGeneratorService = keyGeneratorService;
        this.packetWriterHelper = packetWriterHelper;
        this.sessionInfoService = sessionInfoService;
        this.kafkaBrokerRoutesOffsetExtractor = kafkaBrokerRoutesOffsetExtractor;
        this.typeNameProvider = typeNameProvider;
    }

    public CreateSessionResponse Handle(CreateSessionRequest request)
    {
        SetDefaultType(request);

        var sessionKey = this.keyGeneratorService.GenerateStringKey();
        var partitionOffsetsInfos =   this.kafkaBrokerRoutesOffsetExtractor.Extract(request.DataSource);

        var sessionInfoPacket = CreateSessionInfoPacket(request);
        var newSessionPacket = CreateNewSessionPacket(request, partitionOffsetsInfos, sessionInfoPacket);

        var newSessionPacketDto = new NewSessionPacketDto(
            request.DataSource,
            partitionOffsetsInfos,
            request.UtcOffset?.ToTimeSpan() ?? TimeSpan.Zero,
            new SessionInfoPacketDto(
                sessionInfoPacket.Type,
                sessionInfoPacket.Version,
                sessionInfoPacket.Identifier,
                sessionInfoPacket.AssociateSessionKeys,
                sessionInfoPacket.Details,
                newSessionPacket.UtcOffset.ToTimeSpan(),
                newSessionPacket.CreationTime.ToDateTime(),
                DateTime.UtcNow),
            newSessionPacket.CreationTime.ToDateTime());

        var res = this.sessionInfoService.AddNewSession(sessionKey, newSessionPacketDto);

        if (!res.Success)
        {
            return CreateUnsuccessfulResponse();
        }

        this.WriteNewSessionPacket(request, sessionKey, newSessionPacket);

        return CreateSuccessfulResponse(newSessionPacket, sessionKey);
    }

    private void WriteNewSessionPacket(CreateSessionRequest request, string sessionKey, NewSessionPacket newSessionPacket)
    {
        this.packetWriterHelper.WriteInfoPacket(sessionKey, this.typeNameProvider.NewSessionPacketTypeName, newSessionPacket.ToByteString(), request.DataSource);
    }

    private static CreateSessionResponse CreateSuccessfulResponse(NewSessionPacket newSessionPacket, string sessionKey)
    {
        var response = new CreateSessionResponse
        {
            NewSession = newSessionPacket,
            SessionKey = sessionKey,
            Success = true
        };
        return response;
    }

    private static CreateSessionResponse CreateUnsuccessfulResponse()
    {
        var response = new CreateSessionResponse
        {
            Success = false
        };
        return response;
    }

    private static SessionInfoPacket CreateSessionInfoPacket(CreateSessionRequest request)
    {
        var sessionInfoPacket = new SessionInfoPacket
        {
            Type = request.Type,
            Version = request.Version,
            DataSource = request.DataSource,
            AssociateSessionKeys =
            {
                request.AssociateSessionKey
            },
            Details =
            {
                request.Details
            },
            Identifier = request.Identifier,
            CreationTime = request.CreationTime ?? Timestamp.FromDateTime(DateTime.UtcNow),
            UtcOffset = request.UtcOffset ?? Duration.FromTimeSpan(TimeSpan.Zero),
            TerminationTime = Timestamp.FromDateTime(DateTime.UtcNow)
        };
        return sessionInfoPacket;
    }

    private static NewSessionPacket CreateNewSessionPacket(
        CreateSessionRequest request,
        IEnumerable<TopicPartitionOffsetDto> topicPartitionOffsets,
        SessionInfoPacket sessionInfoPacket)
    {
        var protoTopicOffsets = new MapField<string, long>();
        foreach (var topicPartitionOffset in topicPartitionOffsets)
        {
            protoTopicOffsets.Add(topicPartitionOffset.Title, topicPartitionOffset.Offset);
        }

        var newSessionPacket = new NewSessionPacket
        {
            DataSource = request.DataSource,
            TopicPartitionOffsets =
            {
                protoTopicOffsets
            },
            UtcOffset = sessionInfoPacket.UtcOffset,
            SessionInfo = sessionInfoPacket,
            CreationTime = sessionInfoPacket.CreationTime
        };
        return newSessionPacket;
    }

    private static void SetDefaultType(CreateSessionRequest request)
    {
        if (string.IsNullOrEmpty(request.Type))
        {
            request.Type = "Session";
        }
    }
}
