// <copyright file="Constants.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Core.Routing;

internal static class Constants
{
    public const string SessionInfoTopicName = "SessionInfo";
    public const string SystemStatusTopicName = "SystemStatus";
    public const string SessionInfoDataSourceName = "SessionInfo_Route";
    public const string SystemStatusDataSourceName = "SystemStatus_Route";
    public const string EssentialStreamName = "@stream@::_essential";
    public const string EssentialTopicNameSuffix = "_essential";
}
