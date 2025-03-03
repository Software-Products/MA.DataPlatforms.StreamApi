// <copyright file="PacketReceivedInfoEventArgs.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Abstraction;

public class PacketReceivedInfoEventArgs : EventArgs
{
    public PacketReceivedInfoEventArgs(long connectionId, string dataSource, string stream, string sessionKey, DateTime submitTime, byte[] messageBytes)
    {
        this.ConnectionId = connectionId;
        this.DataSource = dataSource;
        this.Stream = stream;
        this.SessionKey = sessionKey;
        this.SubmitTime = submitTime;
        this.MessageBytes = messageBytes;
    }

    public long ConnectionId { get; }

    public string DataSource { get; }

    public string Stream { get; }

    public string SessionKey { get; }

    public DateTime SubmitTime { get; }

    public byte[] MessageBytes { get; }
}
