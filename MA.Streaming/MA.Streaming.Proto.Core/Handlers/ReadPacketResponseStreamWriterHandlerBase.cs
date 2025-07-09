// <copyright file="ReadPacketResponseStreamWriterHandlerBase.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
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
    internal readonly TimeAndSizeWindowBatchProcessor<PacketReceivedInfoEventArgs>? TimeWindowBatchProcessor;
    internal readonly GenericBuffer<T> WritingBuffer;
    internal bool Started;

    protected ReadPacketResponseStreamWriterHandlerBase(
        ConnectionDetailsDto connectionDetailsDto,
        ServerCallContext context,
        IPacketReaderConnectorService connectorService,
        bool batchingResponses,
        ILogger logger,
        AutoResetEvent autoResetEvent)
    {
        this.ConnectionDetailsDto = connectionDetailsDto;
        this.Context = context;
        this.ConnectorService = connectorService;
        this.BatchingResponses = batchingResponses;
        this.Logger = logger;
        this.AutoResetEvent = autoResetEvent;
        this.ConnectionId = connectionDetailsDto.Id;
        this.ConnectorService.PacketReceived += this.ConnectorService_PacketReceived;
        if (batchingResponses)
        {
            this.TimeWindowBatchProcessor =
                new TimeAndSizeWindowBatchProcessor<PacketReceivedInfoEventArgs>(this.WriteDataToStreamActionAsync, new CancellationTokenSource());
        }
        this.WritingBuffer = new GenericBuffer<T>(this);
    }

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

    public event EventHandler<DateTime>? HandlingStopped;

    public long ConnectionId { get; }

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

    public abstract Task ConsumeAsync(T data);


}
