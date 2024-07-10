// <copyright file="KafkaListenHelper.cs" company="McLaren Applied Ltd.">
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

using Confluent.Kafka;

namespace MA.Streaming.IntegrationTests.Helper;

internal class KafkaListenHelper<TKey, TData>
{
    private readonly string topic;
    private readonly CancellationToken cancellationToken;
    private readonly IConsumer<TKey, TData> consumer;

    public KafkaListenHelper(string server, string topic, CancellationToken cancellationToken)
    {
        this.topic = topic;
        this.cancellationToken = cancellationToken;
        var groupId = Guid.NewGuid().ToString();
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = server,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        this.consumer = new ConsumerBuilder<TKey, TData>(consumerConfig).Build();
        this.consumer.Subscribe(topic);
    }

    public event EventHandler<MessageReceived<TKey, TData>>? OnReceived;

    public Task Start()
    {
        return Task.Run(
            () =>
            {
                while (!this.cancellationToken.IsCancellationRequested)
                {
                    var message = this.consumer.Consume(this.cancellationToken);
                    this.OnReceived?.Invoke(
                        this,
                        new MessageReceived<TKey, TData>(this.topic, message.Message.Key, message.Message.Value, DateTime.Now));
                }
            },
            this.cancellationToken);
    }
}
