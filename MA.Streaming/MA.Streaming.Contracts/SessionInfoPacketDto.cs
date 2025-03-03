// <copyright file="SessionInfoPacketDto.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Contracts;

public class SessionInfoPacketDto
{
    public readonly string Type;
    public readonly uint Version;
    public readonly string Identifier;
    public readonly IReadOnlyList<string> AssociatedKeys;
    public readonly IReadOnlyDictionary<string, string> Details;

    public SessionInfoPacketDto(string type, uint version, string identifier, IReadOnlyList<string> associatedKeys, IReadOnlyDictionary<string, string> details)
    {
        this.Type = type;
        this.Version = version;
        this.Identifier = identifier;
        this.AssociatedKeys = associatedKeys;
        this.Details = details;
    }
}
