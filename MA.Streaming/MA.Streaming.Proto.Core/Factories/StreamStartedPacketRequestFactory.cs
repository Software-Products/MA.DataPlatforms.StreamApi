// <copyright file="StreamStartedPacketRequestFactory.cs" company="Motion Applied Ltd.">
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
using Google.Protobuf.WellKnownTypes;

using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.OpenData;

namespace MA.Streaming.Proto.Core.Factories;

public class StreamStartedPacketRequestFactory : IStreamStartedPacketRequestFactory
{
    private readonly ITypeNameProvider typeNameProvider;

    public StreamStartedPacketRequestFactory(ITypeNameProvider typeNameProvider)
    {
        this.typeNameProvider = typeNameProvider;
    }

    public WriteDataPacketRequestDto CreateStreamStartedPacketWritingRequest(string dataSource, string sessionKey, string streamName, bool isEssential)
    {
        var streamStartedPacket = new StreamStartedPacket
        {
            DataSource = dataSource,
            SessionKey = sessionKey,
            StreamName = streamName,
            StartTime = Timestamp.FromDateTime(DateTime.UtcNow)
        };

        var packet = new Packet
        {
            Content = streamStartedPacket.ToByteString(),
            IsEssential = isEssential,
            SessionKey = sessionKey,
            Type = this.typeNameProvider.StreamStartedPacketTypeName
        };

        return new WriteDataPacketRequestDto(
            new PacketDto(this.typeNameProvider.StreamStartedPacketTypeName, sessionKey, isEssential, streamStartedPacket.ToByteArray()),
            dataSource,
            streamName,
            sessionKey,
            packet.ToByteArray());
    }
}
