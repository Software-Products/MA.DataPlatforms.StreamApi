// <copyright file="StreamingApiConfiguration.cs" company="McLaren Applied Ltd.">
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

using MA.Streaming.Abstraction;

namespace MA.Streaming.Core.Configs;

public class StreamingApiConfiguration : IStreamingApiConfiguration
{
    public StreamingApiConfiguration(
        StreamCreationStrategy? streamCreationStrategy,
        string? brokerUrl,
        IReadOnlyList<PartitionMapping>? partitionMappings,
        int? streamApiPort = 13579,
        bool? integrateSessionManagement = true,
        bool? integrateDataFormatManagement = true,
        bool? useRemoteKeyGenerator = false,
        string? remoteKeyGeneratorServiceAddress = "",
        bool? batchingResponses = false,
        uint? initialisationTimeoutSeconds = 3)
    {
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
}
