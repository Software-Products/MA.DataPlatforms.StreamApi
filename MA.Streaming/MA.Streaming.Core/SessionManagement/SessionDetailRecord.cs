// <copyright file="SessionDetailRecord.cs" company="McLaren Applied Ltd.">
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

// <copyright file="SessionDetailRecord.cs" company="McLaren Applied Ltd.">
// Copyright (c) McLaren Applied Ltd.</copyright>

using MA.Streaming.Contracts;

namespace MA.Streaming.Core.SessionManagement;

public class SessionDetailRecord
{
    public readonly string SessionKey;
    public readonly string DataSource;
    public readonly IReadOnlyList<TopicPartitionOffsetDto> StartingOffsetInfo;
    public readonly long MainOffset;
    public readonly long EssentialOffset;
    public readonly TimeSpan UtcOffset;

    public SessionDetailRecord(
        string sessionKey,
        string dataSource,
        IReadOnlyList<TopicPartitionOffsetDto> startingOffsetInfo,
        long mainOffset,
        long essentialOffset,
        TimeSpan utcOffset,
        ulong updateVersion)
    {
        this.SessionKey = sessionKey;
        this.DataSource = dataSource;
        this.StartingOffsetInfo = startingOffsetInfo;
        this.MainOffset = mainOffset;
        this.EssentialOffset = essentialOffset;
        this.SessionInfoPacket = new SessionInfoPacketDto("", 0, "", [], new Dictionary<string, string>());
        this.Completed = false;
        this.UtcOffset = utcOffset;
    }

    public SessionInfoPacketDto SessionInfoPacket { get; private set; }

    public bool Completed { get; private set; }

    public IReadOnlyList<TopicPartitionOffsetDto> EndingOffsetInfo { get; private set; } = [];

    public void Complete(IReadOnlyList<TopicPartitionOffsetDto> endingOffsetInfo)
    {
        this.Completed = true;
        this.EndingOffsetInfo = endingOffsetInfo;
    }

    public void SetSessionInfo(SessionInfoPacketDto sessionInfoPacket)
    {
        this.SessionInfoPacket = sessionInfoPacket;
    }
}
