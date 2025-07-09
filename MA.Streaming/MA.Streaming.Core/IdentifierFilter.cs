// <copyright file="IdentifierFileter.cs" company="McLaren Applied Ltd.">
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

using System.Text.RegularExpressions;

using MA.Streaming.Abstraction;

namespace MA.Streaming.Core;

public class IdentifierFilter : IIdentifierFilter
{
    private readonly IList<string> defaultIncludeList = [".*"];
    // note that "" returns true at all time for c# regex match
    private readonly IList<string> defaultExludeList = ["(^$)"];
    private IList<string> includeList;
    private IList<string> excludeList;

    public IdentifierFilter()
    {
        this.includeList = this.defaultIncludeList;
        this.excludeList = this.defaultExludeList;
    }

    public void SetIncludeList(IList<string> includeList)
    {
        if (!includeList.Any())
        {
            includeList = this.defaultIncludeList;
        }

        this.includeList = includeList;
    }

    public void SetExcludeList(IList<string> excludeList)
    {
        if (!excludeList.Any())
        {
            excludeList = this.defaultExludeList;
        }

        this.excludeList = excludeList;
    }

    public bool IdentifierInIncludeList(string identifier)
    {
        foreach (var includePattern in this.includeList)
        {
            var r = new Regex(includePattern, RegexOptions.None, TimeSpan.FromMilliseconds(1000));
            if (r.Match(identifier).Success)
            {
                return true;
            }
        }

        return false;
    }

    public bool IdentifierInExcludeList(string identifier)
    {
        foreach (var excludePattern in this.excludeList)
        {
            var r = new Regex(excludePattern, RegexOptions.None, TimeSpan.FromMilliseconds(1000));
            if (r.Match(identifier).Success)
            {
                return true;
            }
        }

        return false;
    }

    public bool ShouldIncludeIdentifier(string identifier)
    {
        // exclude list takes priority
        if (this.IdentifierInExcludeList(identifier))
        {
            return false;
        }

        if (this.IdentifierInIncludeList(identifier))
        {
            return true;
        }

        // if it is not in the include list then exclude it
        return false;
    }
}
