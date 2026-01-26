// <copyright file="DomainKafkaRouteMappingInfoCreatorTests.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Core;
using MA.Streaming.Core.Routing;

using NSubstitute;

using Xunit;

namespace MA.Streaming.UnitTests;

public class DomainKafkaRouteMappingInfoCreatorTests
{
    [Fact]
    public void CreateMainDataTopicName_WithEmptyDomain_ReturnsTopicWithoutDomainPrefix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateMainDataTopicName("TestDataSource");

        // Assert
        Assert.Equal("Data.TestDataSource", result.Topic);
        Assert.Equal("", result.RouteNameInfo.Domain);
        Assert.Equal("TestDataSource", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.MainStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.Main, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateMainDataTopicName_WithNonEmptyDomain_ReturnsTopicWithDomainPrefix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("MyDomain");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateMainDataTopicName("TestDataSource");

        // Assert
        Assert.Equal("MyDomain.Data.TestDataSource", result.Topic);
        Assert.Equal("MyDomain", result.RouteNameInfo.Domain);
        Assert.Equal("TestDataSource", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.MainStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.Main, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateStreamDataTopicName_WithEmptyDomain_TopicBased_ReturnsTopicWithStreamSuffix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateStreamDataTopicName("TestDataSource", "TestStream");

        // Assert
        Assert.Equal("Data.TestDataSource.TestStream", result.Topic);
        Assert.Equal("", result.RouteNameInfo.Domain);
        Assert.Equal("TestDataSource", result.RouteNameInfo.DataSource);
        Assert.Equal("TestStream", result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.Stream, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateStreamDataTopicName_WithNonEmptyDomain_TopicBased_ReturnsTopicWithDomainPrefixAndStreamSuffix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("MyDomain");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateStreamDataTopicName("TestDataSource", "TestStream");

        // Assert
        Assert.Equal("MyDomain.Data.TestDataSource.TestStream", result.Topic);
        Assert.Equal("MyDomain", result.RouteNameInfo.Domain);
        Assert.Equal("TestDataSource", result.RouteNameInfo.DataSource);
        Assert.Equal("TestStream", result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.Stream, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateStreamDataTopicName_PartitionBased_ReturnsTopicWithoutStreamSuffix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("MyDomain");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.PartitionBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateStreamDataTopicName("TestDataSource", "TestStream");

        // Assert
        Assert.Equal("MyDomain.Data.TestDataSource", result.Topic);
        Assert.Equal("MyDomain", result.RouteNameInfo.Domain);
        Assert.Equal("TestDataSource", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.MainStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.Main, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateStreamDataTopicName_EmptyStream_ReturnsTopicWithoutStreamSuffix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("MyDomain");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateStreamDataTopicName("TestDataSource", "");

        // Assert
        Assert.Equal("MyDomain.Data.TestDataSource", result.Topic);
    }

    [Fact]
    public void CreateEssentialTopicName_WithEmptyDomain_ReturnsTopicWithoutDomainPrefix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateEssentialTopicName("TestDataSource");

        // Assert
        Assert.Equal("Essentials.TestDataSource", result.Topic);
        Assert.Equal("", result.RouteNameInfo.Domain);
        Assert.Equal("TestDataSource", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.EssentialStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.Essential, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateEssentialTopicName_WithNonEmptyDomain_ReturnsTopicWithDomainPrefix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("MyDomain");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateEssentialTopicName("TestDataSource");

        // Assert
        Assert.Equal("MyDomain.Essentials.TestDataSource", result.Topic);
        Assert.Equal("MyDomain", result.RouteNameInfo.Domain);
        Assert.Equal("TestDataSource", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.EssentialStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.Essential, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateDeadLetterTopicName_WithEmptyDomain_ReturnsTopicWithoutDomainPrefix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateDeadLetterTopicName();

        // Assert
        Assert.Equal("System.DeadLetter", result.Topic);
        Assert.Equal("", result.RouteNameInfo.Domain);
        Assert.Equal("", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.DeadLetterStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.DeadLetter, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateDeadLetterTopicName_WithNonEmptyDomain_ReturnsTopicWithDomainPrefix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("MyDomain");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateDeadLetterTopicName();

        // Assert
        Assert.Equal("MyDomain.System.DeadLetter", result.Topic);
        Assert.Equal("MyDomain", result.RouteNameInfo.Domain);
        Assert.Equal("", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.DeadLetterStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.DeadLetter, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateSessionInfoTopicName_WithEmptyDomain_ReturnsTopicWithoutDomainPrefix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateSessionInfoTopicName();

        // Assert
        Assert.Equal("System.SessionInfo", result.Topic);
        Assert.Equal("", result.RouteNameInfo.Domain);
        Assert.Equal("", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.SessionInfoStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.SessionInfo, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateSessionInfoTopicName_WithNonEmptyDomain_ReturnsTopicWithDomainPrefix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("MyDomain");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateSessionInfoTopicName();

        // Assert
        Assert.Equal("MyDomain.System.SessionInfo", result.Topic);
        Assert.Equal("MyDomain", result.RouteNameInfo.Domain);
        Assert.Equal("", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.SessionInfoStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.SessionInfo, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateSystemStatusTopicName_WithEmptyDomain_ReturnsTopicWithoutDomainPrefix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateSystemStatusTopicName();

        // Assert
        Assert.Equal("System.Status", result.Topic);
        Assert.Equal("", result.RouteNameInfo.Domain);
        Assert.Equal("", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.SystemStatusStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.SystemStatus, result.RouteNameInfo.RouteType);
    }

    [Fact]
    public void CreateSystemStatusTopicName_WithNonEmptyDomain_ReturnsTopicWithDomainPrefix()
    {
        // Arrange
        var domainProvider = CreateDomainProvider("MyDomain");
        var configProvider = CreateConfigProvider(StreamCreationStrategy.TopicBased);
        var creator = new DomainKafkaRouteMappingInfoCreator(domainProvider, configProvider);

        // Act
        var result = creator.CreateSystemStatusTopicName();

        // Assert
        Assert.Equal("MyDomain.System.Status", result.Topic);
        Assert.Equal("MyDomain", result.RouteNameInfo.Domain);
        Assert.Equal("", result.RouteNameInfo.DataSource);
        Assert.Equal(ConstantSystemKeyStreamNames.SystemStatusStreamName, result.RouteNameInfo.Stream);
        Assert.Equal(RouteType.SystemStatus, result.RouteNameInfo.RouteType);
    }

    private static IDomainNameProvider CreateDomainProvider(string domain)
    {
        var substitute = Substitute.For<IDomainNameProvider>();
        substitute.Provide().Returns(domain);
        return substitute;
    }

    private static IStreamingApiConfigurationProvider CreateConfigProvider(StreamCreationStrategy strategy)
    {
        var config = Substitute.For<IStreamingApiConfiguration>();
        config.StreamCreationStrategy.Returns(strategy);

        var provider = Substitute.For<IStreamingApiConfigurationProvider>();
        provider.Provide().Returns(config);
        return provider;
    }
}
