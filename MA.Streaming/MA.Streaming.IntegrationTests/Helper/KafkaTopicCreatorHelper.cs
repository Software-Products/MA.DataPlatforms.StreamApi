// <copyright file="KafkaTopicCreatorHelper.cs" company="Motion Applied Ltd.">
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

using Confluent.Kafka;
using Confluent.Kafka.Admin;

using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

namespace MA.Streaming.IntegrationTests.Helper;

internal class KafkaTopicCreatorHelper
{
    private readonly IAdminClient adminClient;

    public KafkaTopicCreatorHelper(string server, KafkaSecurityConfig? security = null)
    {
        var adminClientConfig = new AdminClientConfig { BootstrapServers = server };
        KafkaSecurityApplier.Apply(adminClientConfig, security);
        this.adminClient = new AdminClientBuilder(adminClientConfig).Build();
    }

    public void Create(KafkaTopicMetaData metaData)
    {
        try
        {
            this.adminClient.CreateTopicsAsync(
            [
                new TopicSpecification
                {
                    Name = metaData.Topic,
                    ReplicationFactor = metaData.ReplicationFactor ?? 1,
                    NumPartitions = metaData.NumberOfPartitions ?? 1
                }
            ]).Wait();
        }
        catch (AggregateException ex)
            when (ex.InnerExceptions.OfType<CreateTopicsException>()
                    .All(e => e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists)))
        {
            // Topic already exists — not an error.
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
    }
}
