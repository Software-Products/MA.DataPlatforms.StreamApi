// <copyright file="ReadPacketResponseStreamWriterHandlerBase.cs" company="Motion Applied Ltd.">
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
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.PrometheusMetrics;

namespace MA.Streaming.Proto.Core.Handlers;

public abstract class ReadPacketResponseStreamWriterHandlerBase<T> : IBufferConsumer<T>
{
    internal readonly ConnectionDetailsDto ConnectionDetailsDto;
    internal readonly ServerCallContext Context;
    internal readonly IPacketReaderConnectorService ConnectorService;
    internal readonly bool BatchingResponses;
    internal readonly ILogger Logger;
    internal readonly AutoResetEvent AutoResetEvent;
    internal readonly SizeBasedBatchProcessor<PacketReceivedInfoEventArgs>? TimeWindowBatchProcessor;
    internal readonly GenericBuffer<T> WritingBuffer;
    internal bool Started;

    protected ReadPacketResponseStreamWriterHandlerBase(
        ConnectionDetailsDto connectionDetailsDto,
        ServerCallContext context,
        IPacketReaderConnectorService connectorService,
        bool batchingResponses,
        ILogger logger,
        AutoResetEvent autoResetEvent,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider)
    {
        this.ConnectionDetailsDto = connectionDetailsDto;
        this.Context = context;
        this.ConnectorService = connectorService;
        this.BatchingResponses = batchingResponses;
        this.Logger = logger;
        this.AutoResetEvent = autoResetEvent;
        this.ConnectionId = connectionDetailsDto.Id;
        this.ConnectorService.PacketReceived += this.ConnectorService_PacketReceived;
        var config = streamingApiConfigurationProvider.Provide();
        if (batchingResponses)
        {
            this.TimeWindowBatchProcessor =
                new SizeBasedBatchProcessor<PacketReceivedInfoEventArgs>(
                    logger,
                    this.WriteDataToStreamActionAsync,
                    PacketReceivedInfoSizeCalculator.CalculateSize,
                    new CancellationTokenSource(),
                    maxBatchSizeBytes: config.BatchingSizesKb * 1024,
                    timeWindowMilliseconds: config.BatchingTimeMs);
        }

        this.WritingBuffer = new GenericBuffer<T>(this);
    }

    public event EventHandler<DateTime>? HandlingStopped;

    public long ConnectionId { get; }

    public abstract Task ConsumeAsync(T data);

    public void StartHandling()
    {
        if (this.Started)
        {
            return;
        }

        _ = Task.Run(
            () =>
            {
                this.ConnectorService.Start();
            },
            this.Context.CancellationToken);
        this.Started = true;
        this.AutoResetEvent.WaitOne();
    }

    public void StopHandling()
    {
        this.ConnectorService.Stop();
        this.AutoResetEvent.Set();
        this.HandlingStopped?.Invoke(this, DateTime.Now);
    }

    protected internal abstract Task WriteDataToStreamActionAsync(IReadOnlyList<PacketReceivedInfoEventArgs> receivedItems);

    protected internal abstract void WriteDataToStreamAction(PacketReceivedInfoEventArgs receivedItem);

    private void ConnectorService_PacketReceived(object? sender, PacketReceivedInfoEventArgs e)
    {
        MetricProviders.NumberOfDataPacketRead.WithLabels(this.ConnectionId.ToString(), e.DataSource, e.Stream)
            .Inc();
        if (this.BatchingResponses &&
            this.TimeWindowBatchProcessor != null)
        {
            this.TimeWindowBatchProcessor.Add(e);
        }
        else
        {
            this.WriteDataToStreamAction(e);
        }
    }

    protected void PushDataToWritingBuffer(List<PacketResponse> packetsResponse, Dictionary<string, int> streamPacketCounter, T data)
    {
        if (packetsResponse.Count <= 0)
        {
            return;
        }

        this.WritingBuffer.AddData(data);

        foreach (var (key, increment) in streamPacketCounter)
        {
            MetricProviders.NumberOfDataPacketDelivered.WithLabels(this.ConnectionId.ToString(), this.ConnectionDetailsDto.DataSource, key)
                .Inc(increment);
        }
    }
}
