// <copyright file="SampleCustomObject.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Api.UsageSample.ReadAndWriteManagement;

public class SampleCustomObject
{
    private const int SizeOfRunIdBytes = sizeof(long);
    private const int SizeOfDateTimeTicks = sizeof(long);
    private const int SizeOfUintOrder = sizeof(uint);

    public SampleCustomObject(long runId, uint order, uint sizeOfSerialisedContent)
        : this(
            runId,
            DateTime.Now,
            order,
            RandomByteArrayDataGenerator.Create(sizeOfSerialisedContent < MinimumObjectSize ? MinimumObjectSize : sizeOfSerialisedContent))
    {
    }

    public long RunId { get; }

    public DateTime CreationTime { get; }

    private SampleCustomObject(long runId, DateTime creationTime, uint order, byte[] content)
    {
        this.RunId = runId;
        this.CreationTime = creationTime;
        this.Order = order;
        this.Content = content;
    }

    public byte[] Content { get; }

    public uint Order { get; }

    public static SampleCustomObject Deserialize(byte[] serializedBytes)
    {
        var runId = BitConverter.ToInt64(serializedBytes, 0);
        var creationTime = DateTime.FromBinary(BitConverter.ToInt64(serializedBytes, SizeOfRunIdBytes));
        var order = BitConverter.ToUInt32(serializedBytes, SizeOfRunIdBytes + SizeOfDateTimeTicks);
        var content = new byte[serializedBytes.Length - (SizeOfRunIdBytes + SizeOfDateTimeTicks + SizeOfUintOrder)];
        Array.Copy(serializedBytes, SizeOfDateTimeTicks + SizeOfUintOrder, content, 0, content.Length);
        return new SampleCustomObject(runId, creationTime, order, content);
    }

    public static uint MinimumObjectSize => SizeOfRunIdBytes + SizeOfDateTimeTicks + SizeOfUintOrder + 1;

    public byte[] Serialize()
    {
        var result = new byte[SizeOfRunIdBytes + SizeOfDateTimeTicks + SizeOfUintOrder + this.Content.Length];
        Array.Copy(BitConverter.GetBytes(this.RunId), 0, result, 0, SizeOfRunIdBytes);
        Array.Copy(BitConverter.GetBytes(this.CreationTime.ToBinary()), 0, result, SizeOfRunIdBytes, SizeOfDateTimeTicks);
        Array.Copy(BitConverter.GetBytes(this.Order), 0, result, SizeOfRunIdBytes + SizeOfDateTimeTicks, SizeOfUintOrder);
        Array.Copy(this.Content, 0, result, SizeOfRunIdBytes + SizeOfDateTimeTicks + SizeOfUintOrder, this.Content.Length);
        return result;
    }
}
