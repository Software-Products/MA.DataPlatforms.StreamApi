// <copyright file="DataFormatRouteSubscriberFactory.cs" company="McLaren Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouteSubscriberComponent;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core.DataFormatManagement;

public class DataFormatRouteSubscriberFactory : IDataFormatRouteSubscriberFactory
{
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly ICancellationTokenSourceProvider cancellationTokenSourceProvider;
    private readonly ILogger logger;

    public DataFormatRouteSubscriberFactory(
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        ICancellationTokenSourceProvider cancellationTokenSourceProvider,
        ILogger logger)
    {
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.cancellationTokenSourceProvider = cancellationTokenSourceProvider;
        this.logger = logger;
    }

    public IRouteSubscriber Create(IReadOnlyList<KafkaRoute> routes)
    {
        var dataFormatSubscriptionConfigurationProvider = new DataFormatConsumingConfigurationProvider(this.streamingApiConfigurationProvider, routes);
        return new KafkaRouteSubscriber(
            new KafkaListenerFactory(
                dataFormatSubscriptionConfigurationProvider,
                this.cancellationTokenSourceProvider,
                this.logger),
            dataFormatSubscriptionConfigurationProvider,
            this.logger);
    }
}
