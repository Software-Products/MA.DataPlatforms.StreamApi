// <copyright file="ConstantSystemKeyStreamNames.cs" company="Motion Applied Ltd.">
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

public static class ConstantSystemKeyStreamNames
{
    public static readonly string SystemStreamPrefix = "SysStream";
    public static readonly string DeadLetterStreamName = $"{SystemStreamPrefix}.DeadLetterStream";
    public static readonly string SystemStatusStreamName = $"{SystemStreamPrefix}.StatusStream";
    public static readonly string SessionInfoStreamName = $"{SystemStreamPrefix}.SessionInfoStream";
    public static readonly string MainStreamName = $"{SystemStreamPrefix}.MainStream";
    public static readonly string EssentialStreamName = $"{SystemStreamPrefix}.EssentialStream";
}

public static class ConstantNames
{
    public const string DataTopicIdentifier = "Data";
    public const string EssentialsTopicIdentifier = "Essentials";
    public const string SystemTopicIdentifier = "System";
    public const string DeadLetterIdentifier = "DeadLetter";
    public const string StatusIdentifier = "Status";
    public const string SessionInfoIdentifier = "SessionInfo";
}
