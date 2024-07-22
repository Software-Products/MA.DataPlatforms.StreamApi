// <copyright file="PacketWriterHelper.cs" company="McLaren Applied Ltd.">
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

using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Handlers;

public class PacketWriterHelper : IPacketWriterHelper
{
    private readonly IPacketWriterConnectorService packetWriterConnectorService;

    public PacketWriterHelper(IPacketWriterConnectorService packetWriterConnectorService)
    {
        this.packetWriterConnectorService = packetWriterConnectorService;
    }

    public void WriteInfoPacket(string sessionKey, string packetType, ByteString content, string dataSource)
    {
        var packet = new Packet
        {
            Content = content,
            IsEssential = false,
            SessionKey = sessionKey,
            Type = packetType
        };

        this.packetWriterConnectorService.WriteInfoPacket(
            new WriteInfoPacketRequestDto(
                new PacketDto(packetType, sessionKey, false, content.ToByteArray()),
                dataSource,
                InfoTypeDto.SessionInfo,
                packet.ToByteArray()));
    }
}
