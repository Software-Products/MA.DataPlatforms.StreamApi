// <copyright file="SessionPacketWritingLog.cs" company="Motion Applied Ltd.">
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

using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.SessionManagement;

public class SessionPacketWritingLog : ISessionPacketWritingLog
{
    private readonly object streamLock = new();
    private readonly HashSet<string> activeStreams;
    private readonly HashSet<string> endedStreams;
    private readonly ITypeNameProvider typeNameProvider;
    private DateTime lastUpdated;

    public SessionPacketWritingLog(string dataSource, string sessionKey, ITypeNameProvider typeNameProvider)
    {
        this.SessionKey = sessionKey;
        this.DataSource = dataSource;
        this.typeNameProvider = typeNameProvider;
        this.activeStreams = [];
        this.endedStreams = [];
        this.lastUpdated = DateTime.UtcNow;
    }

    public string SessionKey { get; }

    public string DataSource { get; }

    public IReadOnlyList<string> GetActiveStreams()
    {
        lock (this.streamLock)
        {
            return this.activeStreams.Except(this.endedStreams).ToList();
        }
    }

    public DateTime GetWhenLastUpdated()
    {
        lock (this.streamLock)
        {
            return this.lastUpdated;
        }
    }

    public bool IsStreamActive(string streamName)
    {
        lock (this.streamLock)
        {
            return this.activeStreams.Contains(streamName) && !this.endedStreams.Contains(streamName);
        }
    }

    public bool IsStreamEnded(string streamName)
    {
        lock (this.streamLock)
        {
            return this.endedStreams.Contains(streamName) && this.activeStreams.Contains(streamName);
        }
    }

    public void Log(string packetType, string streamName, DateTime timeSent)
    {
        lock (this.streamLock)
        {
            if (packetType == this.typeNameProvider.StreamStartedPacketTypeName)
            {
                this.activeStreams.Add(streamName);
            }
            else if (packetType == this.typeNameProvider.StreamStoppedPacketTypeName)
            {
                this.endedStreams.Add(streamName);
            }

            if (this.lastUpdated < timeSent)
            {
                this.lastUpdated = timeSent;
            }
        }
    }
}
