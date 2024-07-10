// <copyright file="PartitionBasedConsumingConfigurationProvider.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;

namespace MA.Streaming.Core.Routing.PartitionsRouting;

public class PartitionBasedConsumingConfigurationProvider : BaseConsumingConfigurationProvider
{
    public PartitionBasedConsumingConfigurationProvider(
        ConnectionDetailsDto connectionDetailsDto,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IRouteBindingInfoRepository routeBindingInfoRepository)
        : base(connectionDetailsDto, streamingApiConfigurationProvider, routeBindingInfoRepository)
    {
    }

    protected override KafkaRoute GetStreamRoute(string stream)
    {
        var bindingInfo = this.RouteBindingInfoRepository.GetOrAdd(this.ConnectionDetailsDto.DataSource, stream);
        return new KafkaRoute(
            bindingInfo.RouteName,
            this.ConnectionDetailsDto.DataSource,
            this.StreamApiConfig.PartitionMappings?.FirstOrDefault(i => i.Stream == stream)?.Partition);
    }
}
