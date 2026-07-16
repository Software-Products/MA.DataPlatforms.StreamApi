// <copyright file="KafkaSecurityConfigurationProviderAdapter.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Configuration.Core;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;

namespace MA.Streaming.Core;

/// <summary>
/// Adapts the Stream API's <see cref="IKafkaSecurityConfigurationProvider"/> (POCO-based) onto
/// the Secu4 routing library's <see cref="IKafkaSecurityConfigProvider"/> so the security
/// credentials can flow into the Kafka routing components.
///
/// Every Kafka client created through the service container (publishers, consumers, the topic
/// helper and the offset extractor) resolves its security from this single adapter, so a trace
/// emitted here once — and again only when the resolved configuration changes — confirms exactly
/// what is being applied across all clients. The SASL password is never logged.
/// </summary>
public class KafkaSecurityConfigurationProviderAdapter : IKafkaSecurityConfigProvider
{
    private readonly IKafkaSecurityConfigurationProvider? provider;
    private readonly ILogger? logger;
    private readonly object logLock = new();
    private string? lastLoggedFingerprint;

    public KafkaSecurityConfigurationProviderAdapter(IKafkaSecurityConfigurationProvider? provider, ILogger? logger = null)
    {
        this.provider = provider;
        this.logger = logger;
    }

    public KafkaSecurityConfig? Provide()
    {
        var source = this.provider?.Provide();
        var mapped = KafkaSecurityConfigurationMapper.Map(source);
        this.LogIfChanged(source, mapped);
        return mapped;
    }

    private void LogIfChanged(KafkaSecurityConfiguration? source, KafkaSecurityConfig? mapped)
    {
        if (this.logger is null)
        {
            return;
        }

        var fingerprint = mapped is null
            ? "NONE — no SASL/SSL credentials resolved; Kafka clients will connect WITHOUT authentication"
            : $"Protocol={source?.Protocol ?? "SaslPlaintext (default)"}, " +
              $"Mechanism={source?.Mechanism ?? "Plain (default)"}, " +
              $"SaslUsername={source?.SaslUsername ?? "(none)"}, " +
              $"Ssl={(string.IsNullOrWhiteSpace(source?.SslCaLocation) ? "off" : "on")}";

        lock (this.logLock)
        {
            if (fingerprint == this.lastLoggedFingerprint)
            {
                return;
            }

            this.lastLoggedFingerprint = fingerprint;
        }

        this.logger.Trace($"[Kafka security] resolved configuration applied to publisher/consumer clients: {fingerprint}");
    }
}
