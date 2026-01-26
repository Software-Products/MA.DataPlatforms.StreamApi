// <copyright file="SessionPacketWritingLogProvider.cs" company="Motion Applied Ltd.">
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

public class SessionPacketWritingLogProvider : ISessionPacketWritingLogProvider
{
    private readonly Dictionary<(string DataSource, string SessionKey), SessionPacketWritingLog> logs = [];
    private readonly object logLock = new();
    private readonly ITypeNameProvider typeNameProvider;

    public SessionPacketWritingLogProvider(ITypeNameProvider typeNameProvider)
    {
        this.typeNameProvider = typeNameProvider;
    }

    public ISessionPacketWritingLog Provide(string dataSource, string sessionKey)
    {
        lock (this.logLock)
        {
            var key = (dataSource, sessionKey);
            if (this.logs.TryGetValue(key, out var writingLog))
            {
                return writingLog;
            }

            writingLog = new SessionPacketWritingLog(dataSource, sessionKey, this.typeNameProvider);
            this.logs[key] = writingLog;

            return writingLog;
        }
    }
}
