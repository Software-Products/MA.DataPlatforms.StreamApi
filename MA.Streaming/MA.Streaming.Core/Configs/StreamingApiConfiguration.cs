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
        StreamCreationStrategy streamCreationStrategy,
        string brokerUrl,
        IReadOnlyList<PartitionMapping>? partitionMappings,
        int streamApiPort = 13579,
        bool integrateSessionManagement = true,
        bool integrateDataFormatManagement = true,
        bool useRemoteKeyGenerator = false,
        string remoteKeyGeneratorServiceAddress = "",
        bool batchingResponses = false,
        int prometheusMetricPort = 10010)
    {
        this.StreamCreationStrategy = streamCreationStrategy;
        this.BrokerUrl = brokerUrl;
        this.PartitionMappings = partitionMappings;
        this.BatchingResponses = batchingResponses;
        this.PrometheusMetricPort = prometheusMetricPort;
        this.RemoteKeyGeneratorServiceAddress = remoteKeyGeneratorServiceAddress;
        this.UseRemoteKeyGenerator = useRemoteKeyGenerator;
        this.StreamApiPort = streamApiPort;
        this.IntegrateDataFormatManagement = integrateDataFormatManagement;
        this.IntegrateSessionManagement = integrateSessionManagement;
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

    public int PrometheusMetricPort { get; }
}
