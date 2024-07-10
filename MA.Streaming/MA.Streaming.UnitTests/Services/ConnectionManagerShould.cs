// <copyright file="ConnectionManagerShould.cs" company="McLaren Applied Ltd.">
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

using FluentAssertions;

using Grpc.Core;

using MA.Streaming.API;
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core;
using MA.Streaming.Proto.Core.Services;

using NSubstitute;

using Xunit;

namespace MA.Streaming.UnitTests.Services;

public class ConnectionManagerShould
{
    private readonly ConnectionManager connectionManager = new(new ActiveConnectionManager());

    [Fact]
    public async Task AssignUniqueIdToEachConnection()
    {
        // Arrange
        var request = new NewConnectionRequest
        {
            Details = new ConnectionDetails
            {
                DataSource = "DataSource",
                Session = "12345"
            }
        };
        MetricProviders.NumberOfConnections.WithLabels(request.Details.DataSource).Set(0);
       var context = Substitute.For<ServerCallContext>();

        // Act
        var connection1 = await this.connectionManager.NewConnection(request, context);
        var connection2 = await this.connectionManager.NewConnection(request, context);

        // Assert
        connection2.Connection.Id.Should().NotBe(connection1.Connection.Id);
        MetricProviders.NumberOfConnections.WithLabels(request.Details.DataSource).Value.Should().Be(2);
    }

    [Fact]
    public async Task GetConnectionWithInvalidId()
    {
        // Arrange
        var newRequest = new NewConnectionRequest
        {
            Details = new ConnectionDetails
            {
                DataSource = "DataSource",
                Session = "12345"
            }
        };

        var context = Substitute.For<ServerCallContext>();

        await this.connectionManager.NewConnection(newRequest, context);
        const long InvalidId = 0L;

        var getRequest = new GetConnectionRequest
        {
            Connection = new Connection
            {
                Id = InvalidId
            }
        };

        // Act
        var connection = await this.connectionManager.GetConnection(getRequest, context);

        // Assert
        connection.Details.Should().BeNull();
    }

    [Fact]
    public async Task GetConnectionWithValidId()
    {
        // Arrange
        var newRequest = new NewConnectionRequest
        {
            Details = new ConnectionDetails
            {
                DataSource = "DataSource",
                Session = "12345"
            }
        };

        var context = Substitute.For<ServerCallContext>();

        var newConnection = await this.connectionManager.NewConnection(newRequest, context);
        var validId = newConnection.Connection.Id;

        var getRequest = new GetConnectionRequest
        {
            Connection = new Connection
            {
                Id = validId
            }
        };

        // Act
        var connection = await this.connectionManager.GetConnection(getRequest, context);

        // Assert
        connection.Details.Should().BeEquivalentTo(newRequest.Details);
    }

    [Fact]
    public async Task NotRemoveInvalidConnectionWhenClosed()
    {
        // Arrange
        var newRequest = new NewConnectionRequest
        {
            Details = new ConnectionDetails
            {
                DataSource = "DataSource",
                Session = "12345"
            }
        };
        MetricProviders.NumberOfConnections.WithLabels(newRequest.Details.DataSource).Set(0);
        var context = Substitute.For<ServerCallContext>();

        var newConnection = await this.connectionManager.NewConnection(newRequest, context);
        var validId = newConnection.Connection.Id;
        const long InvalidId = 0L;

        var closeRequest = new CloseConnectionRequest
        {
            Connection = new Connection
            {
                Id = InvalidId
            }
        };

        // Act
        var closeResponse =await this.connectionManager.CloseConnection(closeRequest, context);

        // Assert
        closeResponse.Success.Should().BeFalse();

        var getRequest = new GetConnectionRequest
        {
            Connection = new Connection
            {
                Id = validId
            }
        };

        var connection =await this.connectionManager.GetConnection(getRequest, context);
        connection.Details.Should().BeEquivalentTo(newRequest.Details);
        MetricProviders.NumberOfConnections.WithLabels(newRequest.Details.DataSource).Value.Should().Be(1);
    }

    [Fact]
    public async Task RemoveConnectionWhenClosed()
    {
        // Arrange
        var newRequest = new NewConnectionRequest
        {
            Details = new ConnectionDetails
            {
                DataSource = "DataSource",
                Session = "12345"
            }
        };
        MetricProviders.NumberOfConnections.WithLabels(newRequest.Details.DataSource).Set(0);

        var context = Substitute.For<ServerCallContext>();

        var newConnection = await this.connectionManager.NewConnection(newRequest, context);
        var validId = newConnection.Connection.Id;

        var closeRequest = new CloseConnectionRequest
        {
            Connection = new Connection
            {
                Id = validId
            }
        };

        // Act
        var closeResponse = await this.connectionManager.CloseConnection(closeRequest, context);

        // Assert
        closeResponse.Success.Should().BeTrue();

        var getRequest = new GetConnectionRequest
        {
            Connection = new Connection
            {
                Id = validId
            }
        };

        var connection = await this.connectionManager.GetConnection(getRequest, context);
        connection.Details.Should().BeNull();
        MetricProviders.NumberOfConnections.WithLabels(newRequest.Details.DataSource).Value.Should().Be(0);
    }
}
