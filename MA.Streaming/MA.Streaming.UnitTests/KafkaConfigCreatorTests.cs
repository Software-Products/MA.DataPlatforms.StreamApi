// <copyright file="KafkaConfigCreatorTests.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Core.Routing;

using Xunit;

namespace MA.Streaming.UnitTests;

/// <summary>
/// Locks in the Stream API baseline values produced by the config creators,
/// so a future change to DefaultProperties cannot silently drift the producer/consumer baseline.
/// </summary>
public class KafkaConfigCreatorTests
{
    [Fact]
    public void PublishingConfig_UsesStreamApiDefaults_AndInjectedBrokerUrl()
    {
        var config = KafkaPublishingConfigCreator.Create("broker:9092");

        Assert.Equal("broker:9092", config.Server);
        Assert.Equal("zstd",    config.Properties["compression.type"]);
        Assert.Equal("20",      config.Properties["linger.ms"]);
        Assert.Equal("4194304", config.Properties["batch.size"]);
        Assert.Equal("2000000", config.Properties["queue.buffering.max.messages"]);
        Assert.Equal("2097152", config.Properties["queue.buffering.max.kbytes"]);
        Assert.Equal("4194304", config.Properties["message.max.bytes"]);
        Assert.Equal("600000",  config.Properties["message.timeout.ms"]);
        Assert.Equal("1",       config.Properties["acks"]);
        Assert.Equal("1000000", config.Properties["max.in.flight.requests.per.connection"]);
        Assert.Equal("false",   config.Properties["enable.idempotence"]);
        Assert.False(config.Properties.ContainsKey("statistics.interval.ms")); // disabled by default
    }

    [Fact]
    public void PublishingConfig_AppliesStatisticsIntervalWhenProvided()
    {
        var config = KafkaPublishingConfigCreator.Create("broker:9092", KafkaPublishingConfigCreator.EnabledStatisticsIntervalMs);

        Assert.Equal(
            KafkaPublishingConfigCreator.EnabledStatisticsIntervalMs.ToString(),
            config.Properties["statistics.interval.ms"]);
    }

    [Fact]
    public void ListeningConfig_UsesStreamApiDefaults_AndInjectedValues()
    {
        var config = KafkaListeningConfigCreator.Create("broker:9092", "group-1", AutoOffsetResetMode.Earliest, 42);

        Assert.Equal("broker:9092", config.Server);
        Assert.Equal("group-1", config.GroupId);
        Assert.Equal(AutoOffsetResetMode.Earliest, config.AutoOffsetResetMode);
        Assert.Equal(42, config.Offset);
        Assert.Equal("45000", config.Properties["session.timeout.ms"]);
        Assert.False(config.Properties.ContainsKey("statistics.interval.ms")); // disabled by default
    }

    [Fact]
    public void ListeningConfig_AppliesStatisticsIntervalWhenProvided()
    {
        var config = KafkaListeningConfigCreator.Create(
            "broker:9092",
            "group-1",
            AutoOffsetResetMode.Latest,
            null,
            KafkaPublishingConfigCreator.EnabledStatisticsIntervalMs);

        Assert.Equal(
            KafkaPublishingConfigCreator.EnabledStatisticsIntervalMs.ToString(),
            config.Properties["statistics.interval.ms"]);
        Assert.Null(config.Offset);
    }
}
