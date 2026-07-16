// <copyright file="IStreamingApiConfiguration.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Abstraction;

public interface IStreamingApiConfiguration
{
    StreamCreationStrategy StreamCreationStrategy { get; }

    string BrokerUrl { get; }

    IReadOnlyList<PartitionMapping>? PartitionMappings { get; }

    int StreamApiPort { get; }

    bool IntegrateSessionManagement { get; }

    bool IntegrateDataFormatManagement { get; }

    bool UseRemoteKeyGenerator { get; }

    string RemoteKeyGeneratorServiceAddress { get; }

    bool BatchingResponses { get; }

    public uint InitialisationTimeoutSeconds { get; }

    public uint TerminationTimeoutSeconds { get; }

    public string Domain { get; }

    public int BatchingSizesKb { get; }

    public int BatchingTimeMs { get; }

    bool EnableGrpcReflection { get; }

    /// <summary>
    /// When <see langword="true"/>, enables the router's Prometheus metrics for the publish side by
    /// turning on the Kafka producer statistics callback. Defaults to <see langword="false"/> — the
    /// callback never fires and there is zero overhead. The callback is timer-based (not per-message),
    /// so cost does not scale with message volume.
    /// </summary>
    bool EnableRouterPublishMetrics { get; }

    /// <summary>
    /// When <see langword="true"/>, enables the router's Prometheus metrics for the consume side by
    /// turning on the Kafka consumer statistics callback. Defaults to <see langword="false"/>.
    /// </summary>
    bool EnableRouterConsumeMetrics { get; }

    string KafkaBrokerPublishingConfigFilePath { get; }

    string KafkaBrokerConsumingConfigFilePath { get; }

    /// <summary>
    /// Path to the optional JSON whitelist file that controls which keys in the broker override
    /// files are allowed to override the running configuration.
    /// Publish rules are applied to the publisher; consume rules to all consumers.
    /// When the file does not exist no whitelist is enforced (all keys allowed).
    /// Defaults to <c>kafka-broker-whitelist.json</c>.
    /// </summary>
    string KafkaBrokerConfigWhitelistFilePath { get; }

    /// <summary>
    /// Kafka security credentials. Populated from the <c>StreamApiConfig:Security</c> section
    /// of any loaded configuration source, including the dedicated security config file and
    /// environment variables (<c>StreamApiConfig__Security__SaslUsername</c> etc.).
    /// </summary>
    KafkaSecurityConfiguration? Security { get; }
}
