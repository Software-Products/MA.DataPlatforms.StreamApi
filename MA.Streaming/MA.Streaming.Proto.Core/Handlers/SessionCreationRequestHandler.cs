// <copyright file="SessionCreationRequestHandler.cs" company="McLaren Applied Ltd.">
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
using Google.Protobuf.Collections;

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
    private readonly IRouteInfoProvider routeInfoProvider;
    private readonly ITypeNameProvider typeNameProvider;

    public SessionCreationRequestHandler(
        IRouteInfoProvider routeInfoProvider,
        ITypeNameProvider typeNameProvider,
        IKeyGeneratorService keyGeneratorService,
        IPacketWriterHelper packetWriterHelper)

    {
        this.keyGeneratorService = keyGeneratorService;
        this.packetWriterHelper = packetWriterHelper;
        this.routeInfoProvider = routeInfoProvider;
        this.typeNameProvider = typeNameProvider;
    }

    public Task<CreateSessionResponse> Handle(CreateSessionRequest request)
    {
        SetDefaultType(request);

        var sessionKey = this.keyGeneratorService.GenerateStringKey();
        var topicPartitionOffsets = this.GetTopicPartitionOffsets(request.DataSource);

        var newSessionPacket = CreateNewSessionPacket(request, topicPartitionOffsets);
        this.WriteNewSessionPacket(request, sessionKey, newSessionPacket);

        var sessionInfoPacket = CreateSessionInfoPacket(request);
        this.WriteSessionInfoPacket(request, sessionKey, sessionInfoPacket);

        return Task.FromResult(CreateSuccessfulResponse(newSessionPacket, sessionKey));
    }

    private void WriteSessionInfoPacket(CreateSessionRequest request, string sessionKey, SessionInfoPacket sessionInfoPacket)
    {
        this.packetWriterHelper.WriteInfoPacket(sessionKey, this.typeNameProvider.SessionInfoPacketTypeName, sessionInfoPacket.ToByteString(), request.DataSource);
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
            SessionKey = sessionKey
        };
        return response;
    }

    private static SessionInfoPacket CreateSessionInfoPacket(CreateSessionRequest request)
    {
        var sessionInfoPacket = new SessionInfoPacket
        {
            Type = request.Type,
            Version = request.Version,
            DataSource = request.DataSource
        };
        return sessionInfoPacket;
    }

    private static NewSessionPacket CreateNewSessionPacket(CreateSessionRequest request, MapField<string, long> topicPartitionOffsets)
    {
        var newSessionPacket = new NewSessionPacket
        {
            DataSource = request.DataSource,
            TopicPartitionOffsets =
            {
                topicPartitionOffsets
            }
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

    private MapField<string, long> GetTopicPartitionOffsets(string dataSource)
    {
        var result = new MapField<string, long>();
        var routeInfos = this.routeInfoProvider.GetRouteInfo(dataSource).Cast<KafkaRouteInfo>().ToList();
        foreach (var routeInfo in routeInfos)
        {
            result.Add($"{routeInfo.Topic}:{routeInfo.Partition}", routeInfo.Offset);
        }

        return result;
    }
}
