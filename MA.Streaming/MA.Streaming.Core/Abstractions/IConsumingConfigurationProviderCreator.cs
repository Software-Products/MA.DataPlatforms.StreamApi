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
    private readonly object readWriteLock = new();

    public ConsumingConfigurationProviderCreator(
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IKafkaRouteInfoCreator kafkaRouteInfoCreator)
    {
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.kafkaRouteInfoCreator = kafkaRouteInfoCreator;
    }

    public IConsumingConfigurationProvider Create(ConnectionDetailsDto connectionDetailsDto)
    {
        lock (this.readWriteLock)
        {
            return new ConsumingConfigurationProvider(connectionDetailsDto, this.streamingApiConfigurationProvider, this.kafkaRouteInfoCreator);
        }
    }
}
