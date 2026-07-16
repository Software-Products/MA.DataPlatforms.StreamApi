// <copyright file="IConsumingConfigurationProviderCreator.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Configuration.Core;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Routing;

namespace MA.Streaming.Core.Abstractions;

public interface IConsumingConfigurationProviderCreator
{
    IConsumingConfigurationProvider Create(ConnectionDetailsDto connectionDetailsDto);
}

public class ConsumingConfigurationProviderCreator : IConsumingConfigurationProviderCreator
{
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly IKafkaRouteInfoCreator kafkaRouteInfoCreator;
    private readonly IKafkaSecurityConfigProvider kafkaSecurityConfigProvider;
    private readonly string filePath;
    private readonly string? whitelistFilePath;
    private readonly object readWriteLock = new();

    public ConsumingConfigurationProviderCreator(
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IKafkaRouteInfoCreator kafkaRouteInfoCreator,
        IKafkaSecurityConfigProvider kafkaSecurityConfigProvider)
    {
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
        this.kafkaSecurityConfigProvider = kafkaSecurityConfigProvider;
        var config = streamingApiConfigurationProvider.Provide();
        this.filePath = config.KafkaBrokerConsumingConfigFilePath;
        this.whitelistFilePath = config.KafkaBrokerConfigWhitelistFilePath;
    }

    public IConsumingConfigurationProvider Create(ConnectionDetailsDto connectionDetailsDto)
    {
        lock (this.readWriteLock)
        {
            IConsumingConfigurationProvider inner = new ConsumingConfigurationProvider(
                connectionDetailsDto,
                this.streamingApiConfigurationProvider,
                this.kafkaRouteInfoCreator);

            return ApplyFileOverride(inner, this.filePath, this.kafkaSecurityConfigProvider, this.whitelistFilePath);
        }
    }

    internal static IConsumingConfigurationProvider ApplyFileOverride(
        IConsumingConfigurationProvider inner,
        string filePath,
        IKafkaSecurityConfigProvider securityConfigProvider,
        string? whitelistFilePath = null)
    {
        if (File.Exists(filePath))
        {
            var whitelist = RoutingConfigurationProviderCreator.LoadWhitelistRules(whitelistFilePath, "Consume");
            return new JsonKafkaConsumerBrokerOverrideProvider(
                inner,
                filePath,
                excludeFields: ConsumerBrokerFields.BootstrapServers | ConsumerBrokerFields.GroupId,
                securityConfigProvider: securityConfigProvider,
                whitelist: whitelist);
        }

        // No broker file — apply security-only decorator when credentials are set,
        // mirroring the inline security applied on the routing (publish) side.
        var security = securityConfigProvider.Provide();
        return security is not null
            ? new SecurityConsumingConfigurationProviderDecorator(inner, security)
            : inner;
    }
}

/// <summary>
///     Decorator that stamps the given <see cref="KafkaSecurityConfig" /> onto every
///     <see cref="KafkaConsumingConfig" /> produced by the inner provider.
///     Used when no broker-override file is present but a runtime security provider is injected.
/// </summary>
internal sealed class SecurityConsumingConfigurationProviderDecorator : IConsumingConfigurationProvider
{
    private readonly IConsumingConfigurationProvider inner;
    private readonly KafkaSecurityConfig security;

    public SecurityConsumingConfigurationProviderDecorator(IConsumingConfigurationProvider inner, KafkaSecurityConfig security)
    {
        this.inner = inner;
        this.security = security;
    }

    public ConsumingConfiguration Provide()
    {
        var config = this.inner.Provide();
        var secured = config.KafkaConsumingConfigs
            .Select(c => new KafkaConsumingConfig(c.KafkaListeningConfig, c.KafkaRoutes, c.RoutesMetaData, this.security))
            .ToList();
        return new ConsumingConfiguration(secured);
    }
}
