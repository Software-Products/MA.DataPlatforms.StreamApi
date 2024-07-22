// <copyright file="TopicPartitionOffsetDto.cs" company="McLaren Applied Ltd.">
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

public class TopicPartitionOffsetDto
{
    public readonly string TopicName;
    public readonly int Partition;
    public readonly long Offset;

    public TopicPartitionOffsetDto(string topicName, int partition, long offset)
    {
        this.TopicName = topicName;
        this.Partition = partition;
        this.Offset = offset;
    }

    public override string ToString()
    {
        return $"{this.TopicName}:{this.Partition}";
    }
}
