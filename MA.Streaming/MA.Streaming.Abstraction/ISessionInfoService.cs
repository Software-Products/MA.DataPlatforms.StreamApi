// <copyright file="ISessionInfoService.cs" company="McLaren Applied Ltd.">
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

using MA.Streaming.Contracts;

namespace MA.Streaming.Abstraction;

public interface ISessionInfoService
{
    public event EventHandler<SessionsInfoChangeEventArg>? SessionStarted;

    public event EventHandler<SessionsInfoChangeEventArg>? SessionStopped;

    public event EventHandler<SessionsInfoChangeEventArg>? SessionUpdated;

    public event EventHandler<DateTime> ServiceStarted;

    public event EventHandler<DateTime> ServiceStopped;

    (bool Success, string Message) UpdateSessionInfo(string sessionKey, SessionInfoPacketDto sessionInfo);

    (bool Success,string Message) EndSession(string sessionKey, IReadOnlyList<TopicPartitionOffsetDto> partitionOffsetDto);

    (bool Success, string Message) AddNewSession(string sessionKey, NewSessionPacketDto newSessionPacket);

    public void Start();

    public void Stop();
}
