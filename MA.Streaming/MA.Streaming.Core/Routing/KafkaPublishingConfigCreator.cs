// <copyright file="KafkaPublishingConfigCreator.cs" company="Motion Applied Ltd.">
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

public static class KafkaPublishingConfigCreator
{
    /// <summary>
    /// Statistics interval (ms) applied to Kafka clients when router metrics are enabled.
    /// 0 disables the librdkafka statistics callback entirely (no overhead).
    /// </summary>
    public const int EnabledStatisticsIntervalMs = 5000;

    /// <summary>
    /// Stream API producer opinions expressed as raw librdkafka keys. Only values that differ from
    /// Confluent/librdkafka defaults are listed here — everything else uses the Confluent default.
    /// The JSON broker config file is layered on top of these via the override mechanism.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string> DefaultProperties =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["compression.type"]                       = "zstd",
            ["linger.ms"]                              = "20",
            ["batch.size"]                             = "4194304",
            ["queue.buffering.max.messages"]           = "2000000",
            ["queue.buffering.max.kbytes"]             = "2097152",
            ["message.max.bytes"]                      = "4194304",
            ["message.timeout.ms"]                     = "600000",
            ["acks"]                                   = "1",
            ["max.in.flight.requests.per.connection"]  = "1000000",
            ["enable.idempotence"]                     = "false",
        };

    public static KafkaPublishingConfig Create(string brokerUrl, int statisticsIntervalMs = 0)
    {
        var props = new Dictionary<string, string>(DefaultProperties, StringComparer.OrdinalIgnoreCase);
        if (statisticsIntervalMs > 0)
            props["statistics.interval.ms"] = statisticsIntervalMs.ToString();
        return new KafkaPublishingConfig(brokerUrl, props);
    }
}
