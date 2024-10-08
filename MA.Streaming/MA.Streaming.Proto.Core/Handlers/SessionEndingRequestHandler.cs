// <copyright file="SessionEndingRequestHandler.cs" company="McLaren Applied Ltd.">
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

using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core.SessionManagement;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Handlers;

public class SessionEndingRequestHandler : ISessionEndingRequestHandler
{
    private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository;
    private readonly IPacketWriterHelper packetWriterHelper;
    private readonly IRouteInfoProvider routeInfoProvider;
    private readonly ITypeNameProvider typeNameProvider;

    public SessionEndingRequestHandler(
        IRouteInfoProvider routeInfoProvider,
        ITypeNameProvider typeNameProvider,
        IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository,
        IPacketWriterHelper packetWriterHelper)
    {
        this.sessionInfoRepository = sessionInfoRepository;
        this.packetWriterHelper = packetWriterHelper;
        this.routeInfoProvider = routeInfoProvider;
        this.typeNameProvider = typeNameProvider;
    }

    public async Task<EndSessionResponse> Handle(EndSessionRequest request)
    {
        var sessionDetailRecord = this.sessionInfoRepository.Get(request.SessionKey);
        if (sessionDetailRecord is null)
        {
            return await Task.FromResult(new EndSessionResponse());
        }

        var topicPartitionOffsets = this.GetTopicPartitionOffsets(sessionDetailRecord.DataSource);

        var endOfSession = CreateEndOfSessionPacket(sessionDetailRecord, topicPartitionOffsets);
        this.WritePacket(request, endOfSession, sessionDetailRecord);

        return await Task.FromResult(CreateSuccessfulResponse(endOfSession));
    }

    private void WritePacket(EndSessionRequest request, EndOfSessionPacket endOfSession, SessionDetailRecord sessionDetailRecord)
    {
        this.packetWriterHelper.WriteInfoPacket(
            request.SessionKey,
            this.typeNameProvider.EndOfSessionPacketTypeName,
            endOfSession.ToByteString(),
            sessionDetailRecord.DataSource);
    }

    private static EndSessionResponse CreateSuccessfulResponse(EndOfSessionPacket endOfSession)
    {
        var endSessionResponse = new EndSessionResponse
        {
            EndSession = endOfSession
        };
        return endSessionResponse;
    }

    private static EndOfSessionPacket CreateEndOfSessionPacket(SessionDetailRecord sessionDetailRecord, MapField<string, long> topicPartitionOffsets)
    {
        var endOfSession = new EndOfSessionPacket
        {
            DataSource = sessionDetailRecord.DataSource,
            TopicPartitionOffsets =
            {
                topicPartitionOffsets
            }
        };
        return endOfSession;
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
