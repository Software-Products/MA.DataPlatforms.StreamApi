// <copyright file="KafkaPublishHelper.cs" company="McLaren Applied Ltd.">
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

internal class KafkaPublishHelper
{
    private readonly IProducer<string?, byte[]> producer;

    public KafkaPublishHelper(string brokerUrl)
    {
        var configuration = new ProducerConfig
        {
            BootstrapServers = brokerUrl
        };
        this.producer = new ProducerBuilder<string?, byte[]>(configuration).Build();
    }

    public void PublishData(string topic, byte[] data, string? key = "", int partition = 0)
    {
        try
        {
            this.producer.Produce(
                new TopicPartition(topic, partition),
                new Message<string?, byte[]>
                {
                    Key = key,
                    Value = data
                });
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
    }
}
