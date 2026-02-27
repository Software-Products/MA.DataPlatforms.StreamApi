// <copyright file="EssentialRouteConsumingConfigurationProviderTests.cs" company="Motion Applied Ltd.">
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

using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Routing.EssentialsRouting;

using NSubstitute;

using Xunit;

namespace MA.Streaming.UnitTests;

public class EssentialRouteConsumingConfigurationProviderTests
{
    [Fact]
    public void Provide_UsesEssentialsOffset_NotMainOffset()
    {
        // Arrange
        var expectedEssentialsOffset = 1234L;
        var unexpectedMainOffset = 5678L;
        var connectionDetailsDto = new ConnectionDetailsDto(
            id: 1,
            dataSource: "ds1",
            essentialsOffset: expectedEssentialsOffset,
            mainOffset: unexpectedMainOffset,
            sessionKey: "session",
            streamOffsets: new List<long>(),
            streams: new List<string>(),
            excludeMainStream: false,
            "");

        var streamingApiConfig = Substitute.For<IStreamingApiConfiguration>();
        streamingApiConfig.BrokerUrl.Returns("broker-url");

        var streamingApiConfigProvider = Substitute.For<IStreamingApiConfigurationProvider>();
        streamingApiConfigProvider.Provide().Returns(streamingApiConfig);

        var kafkaRouteInfoCreator = Substitute.For<IKafkaRouteInfoCreator>();
        kafkaRouteInfoCreator.CreateEssentialRouteInfo("ds1").Returns(
            new KafkaRouteInfo(new RouteNameInfo("domain1", "ds1", "essential", RouteType.Essential), new KafkaRouteTopicInfo("topic1", 0)));

        var provider = new EssentialRouteConsumingConfigurationProvider(
            connectionDetailsDto,
            streamingApiConfigProvider,
            kafkaRouteInfoCreator);

        // Act
        var config = provider.Provide();

        // Assert
        Assert.NotNull(config);
        Assert.Single(config.KafkaConsumingConfigs);
        var kafkaConfig = config.KafkaConsumingConfigs[0];
        Assert.Equal(expectedEssentialsOffset, kafkaConfig.KafkaListeningConfig.Offset);
        Assert.NotEqual(unexpectedMainOffset, kafkaConfig.KafkaListeningConfig.Offset);
    }
}
