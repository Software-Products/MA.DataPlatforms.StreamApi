// <copyright file="StreamStopPacketWriterTracker.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Core.Abstractions;

namespace MA.Streaming.Core;

public class StreamStopPacketWriterTracker : IStreamStopPacketWriterTracker
{
    private readonly IStreamStoppedPacketRequestFactory streamStoppedPacketRequestFactory;

    private readonly Dictionary<(string dataSource, string sessionKey), HashSet<string>> capturedStreamStoppedPackets = new();

    public StreamStopPacketWriterTracker(IStreamStoppedPacketRequestFactory streamStoppedPacketRequestFactory)
    {
        this.streamStoppedPacketRequestFactory = streamStoppedPacketRequestFactory;
    }

    public StreamStopPacketTrackingResult Track(WriteDataPacketRequestDto request, string refinedStream)
    {
        var dicKey = (request.DataSource, request.SessionKey);
        if (this.capturedStreamStoppedPackets.TryGetValue(dicKey, out var capturedStreamHashSet) &&
            capturedStreamHashSet.Contains(refinedStream))
        {
            return new StreamStopPacketTrackingResult();
        }

        if (capturedStreamHashSet == null)
        {
            capturedStreamHashSet = [];
            this.capturedStreamStoppedPackets[dicKey] = capturedStreamHashSet;
        }

        var dataPacketRequest =
            this.streamStoppedPacketRequestFactory.CreateStreamStoppedPacketWritingRequest(request.DataSource, request.SessionKey, refinedStream, false);

        var essentialPacketRequest = (refinedStream == ConstantSystemKeyStreamNames.MainStreamName)
            ? this.streamStoppedPacketRequestFactory.CreateStreamStoppedPacketWritingRequest(
                request.DataSource,
                request.SessionKey,
                ConstantSystemKeyStreamNames.EssentialStreamName,
                true)
            : null;
        capturedStreamHashSet.Add(refinedStream);
        return essentialPacketRequest != null
            ? new StreamStopPacketTrackingResult(dataPacketRequest, essentialPacketRequest)
            : new StreamStopPacketTrackingResult(dataPacketRequest);
    }
}
