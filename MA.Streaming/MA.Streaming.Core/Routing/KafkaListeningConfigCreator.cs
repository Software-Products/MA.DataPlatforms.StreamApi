// <copyright file="KafkaListeningConfigCreator.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Contracts;

namespace MA.Streaming.Core.Routing;

public static class KafkaListeningConfigCreator
{
    /// <summary>
    /// Stream API consumer opinions expressed as raw librdkafka keys. Only values that differ from
    /// Confluent/librdkafka defaults are listed here — everything else uses the Confluent default.
    /// The JSON broker config file is layered on top of these via the override mechanism.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string> DefaultProperties =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["session.timeout.ms"] = "45000", // Confluent default: 30000
        };

    public static KafkaListeningConfig Create(
        string server,
        string groupId,
        AutoOffsetResetMode autoOffsetResetMode,
        long? offset,
        int statisticsIntervalMs = 0)
    {
        var props = new Dictionary<string, string>(DefaultProperties, StringComparer.OrdinalIgnoreCase);
        if (statisticsIntervalMs > 0)
            props["statistics.interval.ms"] = statisticsIntervalMs.ToString();
        return new KafkaListeningConfig(server, groupId, autoOffsetResetMode, offset, props);
    }
}
