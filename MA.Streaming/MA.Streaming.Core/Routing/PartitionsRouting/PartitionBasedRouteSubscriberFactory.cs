// <copyright file="PartitionBasedRouteSubscriberFactory.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatform.Secu4.RouteSubscriberComponent;
using MA.DataPlatform.Secu4.RouteSubscriberComponent.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.Routing.PartitionsRouting;

public class PartitionBasedRouteSubscriberFactory : IRouteSubscriberFactory
{
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly ILogger logger;

    public PartitionBasedRouteSubscriberFactory(
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        ILogger logger)
    {
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.logger = logger;
    }

    public IRouteSubscriber Create(ConnectionDetailsDto connectionDetailsDto, IRouteBindingInfoRepository routeBindingInfoRepository)
    {
        var basedSubscriptionConfigurationProvider =
            new PartitionBasedConsumingConfigurationProvider(
                connectionDetailsDto,
                this.streamingApiConfigurationProvider,
                routeBindingInfoRepository);
        return new KafkaRouteSubscriber(
            new KafkaListenerFactory(
                basedSubscriptionConfigurationProvider,
                new CancellationTokenSourceProvider(),
                this.logger),
            basedSubscriptionConfigurationProvider,
            this.logger);
    }
}
