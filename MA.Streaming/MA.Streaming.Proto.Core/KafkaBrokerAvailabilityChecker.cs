// <copyright file="KafkaBrokerAvailabilityChecker.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Proto.Core;

internal class KafkaBrokerAvailabilityChecker
{
    public bool Check(string brokerUrl)
    {
        return IsBrokerAvailable(brokerUrl);
    }

    private static bool IsBrokerAvailable(string brokerUrl)
    {
        try
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = brokerUrl
            };
            using var adminClient = new AdminClientBuilder(config).Build();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            if (metadata.Brokers.Count > 0)
            {
                Console.WriteLine("Kafka broker is available. Proceeding with the rest of the code...");
                return true;
            }
            else
            {
                Console.WriteLine("Kafka broker is not available. Retrying in 5 seconds...");
                return false;
            }
        }
        catch (Exception)
        {
            return false;
        }
    }
}
