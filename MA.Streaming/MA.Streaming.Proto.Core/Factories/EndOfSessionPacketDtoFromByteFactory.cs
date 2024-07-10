// <copyright file="EndOfSessionPacketDtoFromByteFactory.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.OpenData;

namespace MA.Streaming.Proto.Core.Factories;

public class EndOfSessionPacketDtoFromByteFactory : IDtoFromByteFactory<EndOfSessionPacketDto>
{
    private readonly ILogger apiLogger;

    public EndOfSessionPacketDtoFromByteFactory(ILogger apiLogger)
    {
        this.apiLogger = apiLogger;
    }

    public EndOfSessionPacketDto? ToDto(byte[] content)
    {
        try
        {
            var endOfSessionPacket = EndOfSessionPacket.Parser.ParseFrom(content);
            return new EndOfSessionPacketDto(
                endOfSessionPacket.DataSource,
                endOfSessionPacket.TopicPartitionOffsets.Select(
                    i =>
                    {
                        var parseInfo = this.ParseTopicPartition(i.Key);
                        return new TopicPartitionOffsetDto(parseInfo.Key, parseInfo.Value, i.Value);
                    }).ToList());
        }
        catch (Exception ex)
        {
            this.apiLogger.Error($"can not parsing the byte array content to the SessionInfoPacket. exception: {ex}");

            return null;
        }
    }

    private KeyValuePair<string, int> ParseTopicPartition(string topicPartitionName)
    {
        if (!topicPartitionName.Contains(":"))
        {
            return new KeyValuePair<string, int>(topicPartitionName, 0);
        }
        else
        {
            var parts = topicPartitionName.Split(':');
            return new KeyValuePair<string, int>(parts[0], int.Parse(parts[1]));
        }
    }
}
