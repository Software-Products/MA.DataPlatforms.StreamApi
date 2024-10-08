// <copyright file="SessionInfo.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Api.UsageSample.SessionManagement;

public class SessionInfo
{
    public string DataSource { get; }

    public string SessionKey { get; }

    public string Type { get; }

    public uint Version { get; }

    public IReadOnlyList<string> AssociatedKeys { get; }

    public string Identifier { get; }

    public bool IsComplete { get; }

    public long MainOffset { get; }

    public long EssentialOffset { get; }

    public IReadOnlyList<string> Streams { get; }

    public IDictionary<string, long> TopicPartitionsOffset { get; }

    public SessionInfo(
        string dataSource,
        string sessionKey,
        string type,
        uint version,
        IReadOnlyList<string> associatedKeys,
        string identifier,
        bool isComplete,
        long mainOffset,
        long essentialOffset,
        IReadOnlyList<string> streams,
        IDictionary<string, long> topicPartitionsOffset)

    {
        this.DataSource = dataSource;
        this.SessionKey = sessionKey;
        this.Type = type;
        this.Version = version;
        this.AssociatedKeys = associatedKeys;
        this.Identifier = identifier;
        this.IsComplete = isComplete;
        this.MainOffset = mainOffset;
        this.EssentialOffset = essentialOffset;
        this.Streams = streams;
        this.TopicPartitionsOffset = topicPartitionsOffset;
    }
}
