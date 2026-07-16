// <copyright file="StreamingApiConfiguration.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Core.Configs;

public class StreamingApiConfiguration : IStreamingApiConfiguration
{
    public StreamingApiConfiguration(
        StreamCreationStrategy? streamCreationStrategy = null,
        string? brokerUrl = null,
        IReadOnlyList<PartitionMapping>? partitionMappings = null,
        int? streamApiPort = 13579,
        bool? integrateSessionManagement = true,
        bool? integrateDataFormatManagement = true,
        bool? useRemoteKeyGenerator = false,
        string? remoteKeyGeneratorServiceAddress = "",
        bool? batchingResponses = false,
        uint? initialisationTimeoutSeconds = 3,
        uint? terminationTimeoutSeconds = 1,
        string? domain = null,
        int? batchingSizesKb = null,
        int? batchingTimeMs = null,
        bool? enableGrpcReflection = false,
        string? kafkaBrokerPublishingConfigFilePath = null,
        string? kafkaBrokerConsumingConfigFilePath = null,
        string? kafkaBrokerConfigWhitelistFilePath = null,
        KafkaSecurityConfiguration? security = null,
        bool? enableRouterPublishMetrics = false,
        bool? enableRouterConsumeMetrics = false)
    {
        this.KafkaBrokerPublishingConfigFilePath = kafkaBrokerPublishingConfigFilePath != null
            ? kafkaBrokerPublishingConfigFilePath.EndsWith("json", StringComparison.Ordinal)
                ? kafkaBrokerPublishingConfigFilePath
                : $"{kafkaBrokerPublishingConfigFilePath}.json"
            : "kafka-publisher-broker.json";
        this.KafkaBrokerConsumingConfigFilePath = kafkaBrokerConsumingConfigFilePath != null
            ? kafkaBrokerConsumingConfigFilePath.EndsWith("json", StringComparison.Ordinal)
                ? kafkaBrokerConsumingConfigFilePath
                : $"{kafkaBrokerConsumingConfigFilePath}.json"
            : "kafka-consumer-broker.json";
        this.StreamCreationStrategy = streamCreationStrategy ?? StreamCreationStrategy.PartitionBased;
        this.BrokerUrl = brokerUrl ?? "localhost:9092";
        this.PartitionMappings = partitionMappings ?? [];
        this.BatchingResponses = batchingResponses ?? false;
        this.InitialisationTimeoutSeconds = initialisationTimeoutSeconds ?? 3;
        this.RemoteKeyGeneratorServiceAddress = remoteKeyGeneratorServiceAddress ?? "";
        this.UseRemoteKeyGenerator = useRemoteKeyGenerator ?? false;
        this.StreamApiPort = streamApiPort ?? 13579;
        this.IntegrateDataFormatManagement = integrateDataFormatManagement ?? true;
        this.IntegrateSessionManagement = integrateSessionManagement ?? true;
        this.Domain = domain ?? "";
        this.TerminationTimeoutSeconds = terminationTimeoutSeconds ?? 1;
        this.BatchingSizesKb = batchingSizesKb ?? 4 * 1024;
        this.BatchingTimeMs = batchingTimeMs ?? 50;
        this.EnableGrpcReflection = enableGrpcReflection ?? false;
        this.KafkaBrokerConfigWhitelistFilePath = kafkaBrokerConfigWhitelistFilePath != null
            ? kafkaBrokerConfigWhitelistFilePath.EndsWith("json", StringComparison.Ordinal)
                ? kafkaBrokerConfigWhitelistFilePath
                : $"{kafkaBrokerConfigWhitelistFilePath}.json"
            : "kafka-broker-whitelist.json";
        this.Security = security;
        this.EnableRouterPublishMetrics = enableRouterPublishMetrics ?? false;
        this.EnableRouterConsumeMetrics = enableRouterConsumeMetrics ?? false;
    }

    public StreamCreationStrategy StreamCreationStrategy { get; }

    public string BrokerUrl { get; }

    public IReadOnlyList<PartitionMapping>? PartitionMappings { get; }

    public int StreamApiPort { get; }

    public bool IntegrateSessionManagement { get; }

    public bool IntegrateDataFormatManagement { get; }

    public bool UseRemoteKeyGenerator { get; }

    public string RemoteKeyGeneratorServiceAddress { get; }

    public bool BatchingResponses { get; }

    public uint InitialisationTimeoutSeconds { get; }

    public uint TerminationTimeoutSeconds { get; }

    public string Domain { get; }

    public int BatchingSizesKb { get; }

    public int BatchingTimeMs { get; }

    public bool EnableGrpcReflection { get; }

    public string KafkaBrokerPublishingConfigFilePath { get; }

    public string KafkaBrokerConsumingConfigFilePath { get; }

    public string KafkaBrokerConfigWhitelistFilePath { get; }

    public KafkaSecurityConfiguration? Security { get; set; }

    public bool EnableRouterPublishMetrics { get; }

    public bool EnableRouterConsumeMetrics { get; }
}
