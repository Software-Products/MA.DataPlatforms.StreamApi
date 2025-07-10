// <copyright file="DataFormatRecord.cs" company="McLaren Applied Ltd.">
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

using MA.Streaming.Contracts;

namespace MA.Streaming.Abstraction;

public class DataFormatRecord
{
    public readonly string DataSource;
    public readonly IReadOnlyList<string> StringIdentifiers;
    public readonly string StringIdentifierKey;
    public readonly DataFormatTypeDto DataFormatTypeDto;
    private readonly HashSet<ulong> identifiers;

    public DataFormatRecord(
        string dataSource,
        string stringIdentifierKey,
        IReadOnlyList<string> stringIdentifiers,
        DataFormatTypeDto dataFormatTypeDto,
        ulong initialIdentifier)
    {
        this.DataSource = dataSource;
        this.StringIdentifiers = stringIdentifiers;
        this.StringIdentifierKey = stringIdentifierKey;
        this.DataFormatTypeDto = dataFormatTypeDto;
        this.identifiers = new HashSet<ulong>
        {
            initialIdentifier
        };
        this.Identifiers = this.identifiers.ToList();
    }

    public IReadOnlyList<ulong> Identifiers { get; private set; }

    public void AddIdentifier(ulong identifier)
    {
        this.identifiers.Add(identifier);
        this.Identifiers = this.identifiers.ToList();
    }
}
