// <copyright file="MonitorResult.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Core;

public class MonitorResult
{
    public readonly MonitorResultStatus Status;

    public readonly string Message;

    private MonitorResult(MonitorResultStatus status, string message = "")
    {
        this.Status = status;
        this.Message = message;
    }

    public static MonitorResult ValidPacket => new MonitorResult(MonitorResultStatus.ValidPacket);

    public static MonitorResult InvalidPacket(string message) => new MonitorResult(MonitorResultStatus.InvalidPacket, message);
}
