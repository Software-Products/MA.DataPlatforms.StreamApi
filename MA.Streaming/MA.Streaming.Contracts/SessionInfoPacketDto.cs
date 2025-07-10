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

    public SessionInfoPacketDto(
        string type,
        uint version,
        string identifier,
        IReadOnlyList<string> associatedKeys,
        IReadOnlyDictionary<string, string> details)
    {
        this.Type = type;
        this.Version = version;
        this.Identifier = identifier;
        this.AssociatedKeys = associatedKeys;
        this.Details = details;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(this.Type, this.Version, this.Identifier, this.AssociatedKeys, this.Details);
    }

    public override bool Equals(object? obj)
    {
        if (obj is not SessionInfoPacketDto other)
        {
            return false;
        }

        return this.Type == other.Type &&
               this.Version == other.Version &&
               this.Identifier == other.Identifier &&
               this.AssociatedKeys.SequenceEqual(other.AssociatedKeys) &&
               AreDictionariesEqual(this.Details, other.Details);
    }

    private static bool AreDictionariesEqual(IReadOnlyDictionary<string, string> dict1, IReadOnlyDictionary<string, string> dict2)
    {
        if (dict1.Count != dict2.Count)
        {
            return false;
        }

        foreach (var kvp in dict1)
        {
            if (!dict2.TryGetValue(kvp.Key, out var value) ||
                value != kvp.Value)
            {
                return false;
            }
        }

        return true;
    }
}
