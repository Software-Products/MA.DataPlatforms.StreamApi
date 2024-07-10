// <copyright file="KafkaClearHelper.cs" company="McLaren Applied Ltd.">
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

public class KafkaClearHelper
{
    private readonly string server;

    public KafkaClearHelper(string server)
    {
        this.server = server;
    }

    public async Task Clear()
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = this.server // Replace with your Kafka broker address
        };

        using var adminClient = new AdminClientBuilder(adminConfig).Build();
        try
        {
            var topics = adminClient.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Select(i => i.Topic).ToList();
            if (topics.Any())
            {
                await adminClient.DeleteTopicsAsync(topics);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error deleting topics: {ex.Message}");
        }
    }
}
