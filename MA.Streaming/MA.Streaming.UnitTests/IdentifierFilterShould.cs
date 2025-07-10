// <copyright file="IdentifierFilterShould.cs" company="McLaren Applied Ltd.">
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

public class IdentifierFilterShould
{
    [Fact]
    public void DefaultIncludeListIncludeAllParameters()
    {
        // Arrange
        var identifierFilter = new IdentifierFilter();
        const string ParameterIdentifier = "Parameter 9";

        // Act 
        var parameterInIncludeList = identifierFilter.IdentifierInIncludeList(ParameterIdentifier);

        // Assert 
        parameterInIncludeList.Should().BeTrue();
    }

    [Fact]
    public void DefaultExcludeListExcludeAllParameters()
    {
        // Arrange
        var identifierFilter = new IdentifierFilter();
        const string ParameterIdentifier = "Parameter 9";

        // Act 
        var parameterInExcludeList = identifierFilter.IdentifierInExcludeList(ParameterIdentifier);

        // Assert 
        parameterInExcludeList.Should().BeFalse();
    }

    [Fact]
    public void CheckParameterInIncludeList()
    {
        // Arrange
        var identifierFilter = new IdentifierFilter();
        List<string> includeList = ["Param.+"];
        const string ParameterIdentifier = "Parameter 9";

        // Act 
        identifierFilter.SetIncludeList(includeList);
        var parameterInIncludeList = identifierFilter.IdentifierInIncludeList(ParameterIdentifier);

        // Assert 
        parameterInIncludeList.Should().BeTrue();
    }

    [Fact]
    public void CheckParameterInExcludeList()
    {
        // Arrange
        var identifierFilter = new IdentifierFilter();
        List<string> excludeList = ["Param.+"];
        const string ParameterIdentifier = "Parameter 9";

        // Act 
        identifierFilter.SetExcludeList(excludeList);
        var parameterInExcludeList = identifierFilter.IdentifierInExcludeList(ParameterIdentifier);

        // Assert 
        parameterInExcludeList.Should().BeTrue();
    }

    [Fact]
    public void ExcludeListTakesPriority()
    {
        // Arrange
        var identifierFilter = new IdentifierFilter();
        List<string> includeList = ["Param.+"];
        List<string> excludeList = ["Param.+"];
        const string ParameterIdentifier = "Parameter 9";

        // Act 
        identifierFilter.SetIncludeList(includeList);
        identifierFilter.SetExcludeList(excludeList);
        var shouldIncludeIdentifier = identifierFilter.ShouldIncludeIdentifier(ParameterIdentifier);

        // Assert 
        shouldIncludeIdentifier.Should().BeFalse();
    }
}
