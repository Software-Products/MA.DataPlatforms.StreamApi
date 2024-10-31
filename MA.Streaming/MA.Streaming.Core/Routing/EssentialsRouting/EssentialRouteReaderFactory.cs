// <copyright file="EssentialRouteReaderFactory.cs" company="McLaren Applied Ltd.">
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

using System.Collections.Concurrent;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouteReaderComponent;
using MA.DataPlatforms.Secu4.RouteReaderComponent.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;

namespace MA.Streaming.Core.Routing.EssentialsRouting;

public class EssentialRouteReaderFactory : IEssentialRouteReaderFactory
{
    private readonly IStreamingApiConfigurationProvider streamingApiConfigurationProvider;
    private readonly IRouteBindingInfoRepository routeBindingInfoRepository;
    private readonly IEssentialTopicNameCreator essentialTopicNameCreator;
    private readonly ICancellationTokenSourceProvider cancellationTokenSourceProvider;
    private readonly ILogger logger;
    private readonly ConcurrentDictionary<long, IRouteReader> readers = new();

    public EssentialRouteReaderFactory(
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IRouteBindingInfoRepository routeBindingInfoRepository,
        IEssentialTopicNameCreator essentialTopicNameCreator,
        ICancellationTokenSourceProvider cancellationTokenSourceProvider,
        ILogger logger)
    {
        this.streamingApiConfigurationProvider = streamingApiConfigurationProvider;
        this.routeBindingInfoRepository = routeBindingInfoRepository;
        this.essentialTopicNameCreator = essentialTopicNameCreator;
        this.cancellationTokenSourceProvider = cancellationTokenSourceProvider;
        this.logger = logger;
    }

    public IRouteReader Create(ConnectionDetailsDto connectionDetailsDto)
    {
        return this.readers.GetOrAdd(connectionDetailsDto.Id, _ => this.CreateRouteReader(connectionDetailsDto));
    }

    private IRouteReader CreateRouteReader(ConnectionDetailsDto connectionDetailsDto)
    {
        var basedSubscriptionConfigurationProvider =
            new EssentialRouteConsumingConfigurationProvider(
                connectionDetailsDto,
                this.streamingApiConfigurationProvider,
                this.routeBindingInfoRepository,
                this.essentialTopicNameCreator);
        return new KafkaRouteReader(
            new KafkaReaderFactory(
                basedSubscriptionConfigurationProvider,
                this.cancellationTokenSourceProvider,
                this.logger),
            basedSubscriptionConfigurationProvider,
            this.logger);
    }
}
