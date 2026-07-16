// <copyright file="KafkaSecurityConfiguration.cs" company="Motion Applied Ltd.">
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

/// <summary>
/// Kafka security credentials. A string-based mirror of <c>KafkaSecurityConfig</c> from
/// MA.DataPlatforms.Secu4, designed for binding from <c>IConfiguration</c> (JSON files and
/// environment variables). All enum-typed fields are represented as strings so that
/// standard .NET configuration binding works without additional converters.
///
/// Populated from the <c>StreamApiConfig:Security</c> configuration section:
/// <code>
///   StreamApiConfig__Security__SaslUsername
///   StreamApiConfig__Security__SaslPassword
///   StreamApiConfig__Security__Protocol
///   StreamApiConfig__Security__Mechanism
///   StreamApiConfig__Security__SslCaLocation
///   StreamApiConfig__Security__SslCertificateLocation
///   StreamApiConfig__Security__SslKeyLocation
///   StreamApiConfig__Security__SslKeyPassword
/// </code>
/// </summary>
public class KafkaSecurityConfiguration
{
    // -- SASL ----------------------------------------------------------------

    /// <summary>SASL username. When set, security is applied using <see cref="Protocol"/> and <see cref="Mechanism"/>.</summary>
    public string? SaslUsername { get; set; }

    /// <summary>SASL password.</summary>
    public string? SaslPassword { get; set; }

    // -- Protocol / Mechanism ------------------------------------------------

    /// <summary>
    /// Security protocol. Accepted values: <c>Plaintext</c>, <c>Ssl</c>, <c>SaslPlaintext</c>, <c>SaslSsl</c>.
    /// Defaults to <c>SaslPlaintext</c> when <see cref="SaslUsername"/> is provided.
    /// </summary>
    public string? Protocol { get; set; }

    /// <summary>
    /// SASL mechanism. Accepted values: <c>Plain</c>, <c>ScramSha256</c>, <c>ScramSha512</c>.
    /// Defaults to <c>Plain</c> when <see cref="SaslUsername"/> is provided.
    /// </summary>
    public string? Mechanism { get; set; }

    // -- SSL -----------------------------------------------------------------

    /// <summary>Path to the CA certificate file for verifying the broker's SSL certificate.</summary>
    public string? SslCaLocation { get; set; }

    /// <summary>Path to the client SSL certificate file (PEM format) for mutual TLS authentication.</summary>
    public string? SslCertificateLocation { get; set; }

    /// <summary>Path to the client SSL private key file (PEM format) for mutual TLS authentication.</summary>
    public string? SslKeyLocation { get; set; }

    /// <summary>Password for the client SSL private key file.</summary>
    public string? SslKeyPassword { get; set; }
}
