// <copyright file="BatchPublisher.cs" company="Motion Applied Ltd.">
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

using Grpc.Core;

using MA.Common.Abstractions;
using MA.Streaming.API;
using MA.Streaming.Proto.Core.Handlers;

namespace MA.Streaming.IntegrationTests;

internal class BatchPublisher
{
    private readonly AsyncClientStreamingCall<WriteDataPacketsRequest, WriteDataPacketsResponse> streamWriter;
    private readonly CancellationToken cancellationToken;
    private readonly SizeBasedBatchProcessor<DataPacketDetails> sizeBatcher;
    private int inputCounter;
    private bool finished;
    private int outputCounter;

    public event EventHandler<DateTime>? DataPacketPublishCompleted;

    public BatchPublisher(ILogger logger, AsyncClientStreamingCall<WriteDataPacketsRequest, WriteDataPacketsResponse> streamWriter, CancellationToken cancellationToken)
    {
        this.streamWriter = streamWriter;
        this.cancellationToken = cancellationToken;
        this.sizeBatcher = new SizeBasedBatchProcessor<DataPacketDetails>(logger,
            this.SendData,
            SizeCalculation,
            cancellationToken,
            maxBatchSizeBytes: 4 * 1024 * 1024);
    }

    public void Add(DataPacketDetails dataPacket)
    {
        this.inputCounter++;
        this.sizeBatcher.Add(dataPacket);
    }

    private static int SizeCalculation(DataPacketDetails arg)
    {
        return arg.CalculateSize();
    }

    private async Task SendData(IReadOnlyList<DataPacketDetails> arg)
    {
        this.outputCounter += arg.Count;
        var request = new WriteDataPacketsRequest
        {
            Details =
            {
                arg
            }
        };
        await this.streamWriter.RequestStream.WriteAsync(request, this.cancellationToken);
        if (this.finished &&
            this.outputCounter == this.inputCounter)
        {
            this.DataPacketPublishCompleted?.Invoke(this, DateTime.UtcNow);
        }
    }

    public void Finish()
    {
        this.finished = true;
        this.sizeBatcher.CompleteAsync().Wait(this.cancellationToken);
    }
}
