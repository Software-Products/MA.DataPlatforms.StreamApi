// <copyright file="MicrosoftLoggerAdapter.cs" company="McLaren Applied Ltd.">
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

using Microsoft.Extensions.Logging;

using ILogger = MA.Common.Abstractions.ILogger;

namespace MA.Streaming.Proto.ServerComponent;

public class MicrosoftLoggerAdapter : ILogger
{
    private readonly ILogger<MicrosoftLoggerAdapter> logger;

    public MicrosoftLoggerAdapter(ILogger<MicrosoftLoggerAdapter> logger)
    {
        this.logger = logger;
    }

    public void Debug(string message)
    {
        this.logger.LogDebug(message);
    }

    public void Error(string message)
    {
        this.logger.LogError(message);
    }

    public void Info(string message)
    {
        this.logger.LogInformation(message);
    }

    public void Trace(string message)
    {
        this.logger.LogTrace(message);
    }

    public void Warning(string message)
    {
        this.logger.LogWarning(message);
    }
}
