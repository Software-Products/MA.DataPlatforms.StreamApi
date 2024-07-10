// <copyright file="ConnectionDetailsDto.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Contracts;

public class ConnectionDetailsDto
{
    public ConnectionDetailsDto(
        long id,
        string dataSource,
        long essentialsOffset,
        long mainOffset,
        string session,
        IReadOnlyList<long> streamOffsets,
        IReadOnlyList<string> streams)
    {
        this.Id = id;
        this.DataSource = dataSource;
        this.EssentialsOffset = essentialsOffset;
        this.MainOffset = mainOffset;
        this.Session = session;
        this.StreamOffsets = streamOffsets;
        this.Streams = streams;
    }

    public long Id { get; }

    public string DataSource { get; }

    public long EssentialsOffset { get; }

    public long MainOffset { get; }

    public string Session { get; }

    public IReadOnlyList<long> StreamOffsets { get; }

    public IReadOnlyList<string> Streams { get; }
}
