// <copyright file="RunInfo.cs" company="McLaren Applied Ltd.">
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

using Grpc.Core;

using MA.Streaming.API;

namespace MA.Streaming.Api.UsageSample.ReadAndWriteManagement;

public class RunInfo
{
    private int receivedCounter;
    private int publishCounter;
    private readonly IClientStreamWriter<WriteDataPacketsRequest> writerStream;

    public event EventHandler<DateTime>? ReceivedCompleted;

    public event EventHandler<DateTime>? PublishedCompleted;

    public event EventHandler<IReadOnlyList<PacketResponse>>? MessageReceived;

    public RunInfo(
        long runId,
        string dataSource,
        string stream,
        string sessionKey,
        uint numberOfMessageToPublish,
        uint messageSize,
        PacketWriterService.PacketWriterServiceClient packetWriterServiceClient)
    {
        this.RunId = runId;
        this.DataSource = dataSource;
        this.Stream = stream;
        this.SessionKey = sessionKey;
        this.NumberOfMessageToPublish = numberOfMessageToPublish;
        this.MessageSize = messageSize;
        this.writerStream = packetWriterServiceClient.WriteDataPackets().RequestStream;
    }

    public long RunId { get; }

    public string DataSource { get; }

    public string Stream { get; }

    public string SessionKey { get; }

    public uint NumberOfMessageToPublish { get; }

    public uint MessageSize { get; }

    public bool Completed { get; private set; }

    public double ElapsedTime { get; private set; }

    public DateTime RunStartingTime { get; private set; }

    public int PublishCounter => this.publishCounter;

    private void IncrementPublishCounter(int value)
    {
        Interlocked.Add(ref this.publishCounter, value);
    }

    public async Task Publish(WriteDataPacketsRequest writeDataPacketsRequest)
    {
        if (this.publishCounter == 0)
        {
            this.RunStartingTime = DateTime.Now;
        }

        try
        {
            await this.writerStream.WriteAsync(writeDataPacketsRequest);
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception.ToString());
        }

        this.IncrementPublishCounter(writeDataPacketsRequest.Details.Count);
        if (this.PublishCounter != this.NumberOfMessageToPublish)
        {
            return;
        }

        await this.writerStream.CompleteAsync();
        this.PublishedCompleted?.Invoke(this, DateTime.Now);
    }

    public string Title
    {
        get
        {
            var state = !this.Completed ? "running" : "completed";
            return $"Id:{this.RunId}-({this.DataSource}:{this.Stream}[{this.SessionKey}])->{state}";
        }
    }

    public void OnMessageReceived(ReadPacketsResponse readPacket)
    {
        Interlocked.Add(ref this.receivedCounter, readPacket.Response.Count);
        this.MessageReceived?.Invoke(this, readPacket.Response);
        if (this.receivedCounter != this.NumberOfMessageToPublish)
        {
            return;
        }

        var finishTime = DateTime.Now;
        this.ElapsedTime = (finishTime - this.RunStartingTime).TotalMilliseconds;
        this.ReceivedCompleted?.Invoke(this, finishTime);
        this.Completed = true;
    }
}
