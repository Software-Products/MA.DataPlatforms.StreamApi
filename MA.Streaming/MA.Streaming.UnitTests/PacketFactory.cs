// <copyright file="PacketFactory.cs" company="Motion Applied Ltd.">
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

using MA.Streaming.OpenData;

namespace MA.Streaming.UnitTests;

public static class PacketFactory
{
    private static readonly Random Random = new();
    private static ulong counter;

    public static Packet CreatePacket(int approximateSizeBytes)
    {
        const int Overhead = 62;
        var dataSize = Math.Max(0, approximateSizeBytes - Overhead);
        var randomBytes = new byte[dataSize];
        Random.NextBytes(randomBytes);

        var packet = new Packet
        {
            Id = counter++,
            Content = ByteString.CopyFrom(randomBytes),
            IsEssential = false,
            SessionKey = Guid.NewGuid().ToString(),
            Type = "Session"
        };

        return packet;
    }
}
