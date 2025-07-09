// <copyright file="GetSessionInfoRequestHandler.cs" company="McLaren Applied Ltd.">
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

using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.SessionManagement;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Handlers;

public class GetSessionInfoRequestHandler : IGetSessionInfoRequestHandler
{
    private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository;
    private readonly IStreamsProvider streamsProvider;

    public GetSessionInfoRequestHandler(
        IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository,
        IStreamsProvider streamsProvider)
    {
        this.sessionInfoRepository = sessionInfoRepository;
        this.streamsProvider = streamsProvider;
    }

    public GetSessionInfoResponse GetSessionInfo(GetSessionInfoRequest request)
    {
        var foundSessionDetail = this.sessionInfoRepository.Get(request.SessionKey);
        if (foundSessionDetail == null)
        {
            return new GetSessionInfoResponse { Success = false };
        }

        var streams = this.streamsProvider.Provide(foundSessionDetail.DataSource);

        var response = new GetSessionInfoResponse
        {
            Type = foundSessionDetail.SessionInfoPacket.Type,
            DataSource = foundSessionDetail.DataSource,
            AssociateSessionKeys =
            {
                foundSessionDetail.SessionInfoPacket.AssociatedKeys
            },
            Identifier = foundSessionDetail.SessionInfoPacket.Identifier,
            Version = foundSessionDetail.SessionInfoPacket.Version,
            IsComplete = foundSessionDetail.Completed,
            MainOffset = foundSessionDetail.MainOffset,
            EssentialsOffset = foundSessionDetail.EssentialOffset,
            TopicPartitionOffsets =
            {
                GetTopicPartitionOffsets(foundSessionDetail.StartingOffsetInfo)
            },
            Streams =
            {
                streams
            },
            UtcOffset = Duration.FromTimeSpan(foundSessionDetail.UtcOffset),
            Success = true
        };
        response.Details.Add(foundSessionDetail.SessionInfoPacket.Details.ToDictionary(detail => detail.Key, detail => detail.Value));

        return response;
    }

    private static MapField<string, long> GetTopicPartitionOffsets(IEnumerable<TopicPartitionOffsetDto> startingOffsetInfo)
    {
        var result = new MapField<string, long>();

        foreach (var topicPartitionOffsetDto in startingOffsetInfo)
        {
            result.Add(topicPartitionOffsetDto.ToString(), topicPartitionOffsetDto.Offset);
        }

        return result;
    }
}
