// <copyright file="ParameterListKeyCreatorShould.cs" company="McLaren Applied Ltd.">
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

using MA.Streaming.Core;

using Xunit;

namespace MA.Streaming.UnitTests;

public class CreateMethodTests
{
    private readonly ParameterListKeyIdentifierCreator parameterListKeyIdentifierCreator = new();

    [Fact]
    public void Create_ShouldReturnEmptyString_WhenListIsEmpty()
    {
        // Act
        var result = this.parameterListKeyIdentifierCreator.Create([]);

        // Assert
        result.Should().Be("");
    }

    [Fact]
    public void Create_ShouldReturnSingleElement_WhenListHasOneItem()
    {
        // Arrange
        var parameters = new List<string>
        {
            "p1"
        };

        // Act
        var result = this.parameterListKeyIdentifierCreator.Create(parameters);

        // Assert
        result.Should().Be("p1");
    }

    [Fact]
    public void Create_ShouldJoinElementsWithComma_WhenListHasMultipleItems()
    {
        // Arrange
        var parameters = new List<string>
        {
            "p1",
            "p2",
            "p3"
        };

        // Act
        var result = this.parameterListKeyIdentifierCreator.Create(parameters);

        // Assert
        result.Should().Be("p1,p2,p3");
    }
}
