// <copyright file="KafkaSecurityConfigurationMapper.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Abstraction;

namespace MA.Streaming.Core;

/// <summary>
/// Maps the Stream API's string-based <see cref="KafkaSecurityConfiguration"/> POCO onto the
/// Secu4 enum-based <see cref="KafkaSecurityConfig"/>. Returns <c>null</c> when no SASL or SSL
/// credential is set, so unsecured deployments are unaffected.
/// </summary>
public static class KafkaSecurityConfigurationMapper
{
    public static KafkaSecurityConfig? Map(KafkaSecurityConfiguration? security)
    {
        if (security is null || !HasAnyCredential(security))
        {
            return null;
        }

        var protocol = ParseEnum(security.Protocol, KafkaSecurityProtocol.SaslPlaintext);
        var mechanism = ParseEnum(security.Mechanism, KafkaSaslMechanism.Plain);

        return new KafkaSecurityConfig(
            protocol: protocol,
            saslMechanism: mechanism,
            saslUsername: security.SaslUsername,
            saslPassword: security.SaslPassword,
            sslCaLocation: security.SslCaLocation,
            sslCertificateLocation: security.SslCertificateLocation,
            sslKeyLocation: security.SslKeyLocation,
            sslKeyPassword: security.SslKeyPassword);
    }

    private static bool HasAnyCredential(KafkaSecurityConfiguration security) =>
        !string.IsNullOrWhiteSpace(security.SaslUsername) ||
        !string.IsNullOrWhiteSpace(security.SslCaLocation) ||
        !string.IsNullOrWhiteSpace(security.SslCertificateLocation);

    private static T ParseEnum<T>(string? value, T defaultValue)
        where T : struct, Enum
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return defaultValue;
        }

        return Enum.TryParse<T>(value, ignoreCase: true, out var result)
            ? result
            : defaultValue;
    }
}
