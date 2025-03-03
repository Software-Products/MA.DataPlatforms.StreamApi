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

using MA.Streaming.Abstraction;

namespace MA.Streaming.Proto.ServerComponent;

public class KafkaBrokerAvailabilityChecker : IKafkaBrokerAvailabilityChecker
{
    private const double TimeoutWaitingForMetadataSeconds = 2;
    private const double TimeBetweenRetriesSeconds = 5;

    public bool Check(string brokerUrl)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = brokerUrl,
        };
        using var adminClient = new AdminClientBuilder(config).Build();

        var brokerAvailable = IsBrokerAvailable(adminClient);

        if (brokerAvailable)
        {
            Console.WriteLine("Kafka broker is available. Proceeding with the rest of the code...");
        }
        else
        {
            Console.WriteLine("Kafka broker is not available");
        }

        return brokerAvailable;
    }

    public Task CheckContinuously(string brokerUrl, CancellationTokenSource cancellationTokenSource)
    {
        return Task.Run(
            async () =>
            {
                var config = new AdminClientConfig
                {
                    BootstrapServers = brokerUrl,
                };
                using var adminClient = new AdminClientBuilder(config).Build();

                var kafkaAvailable = IsBrokerAvailable(adminClient);
                while (!cancellationTokenSource.IsCancellationRequested)
                {
                    while (kafkaAvailable && !cancellationTokenSource.IsCancellationRequested)
                    {
                        if (!IsBrokerAvailable(adminClient))
                        {
                            kafkaAvailable = false;
                            continue;
                        }

                        await Task.Delay(TimeSpan.FromSeconds(TimeBetweenRetriesSeconds), cancellationTokenSource.Token);
                    }

                    while (!kafkaAvailable &&
                           !cancellationTokenSource.IsCancellationRequested)
                    {
                        if (IsBrokerAvailable(adminClient))
                        {
                            Console.WriteLine("Kafka broker is available again");
                            kafkaAvailable = true;
                            continue;
                        }

                        await Task.Delay(TimeSpan.FromSeconds(TimeBetweenRetriesSeconds), cancellationTokenSource.Token);
                    }
                }
            },
            cancellationTokenSource.Token);
    }

    private static bool IsBrokerAvailable(IAdminClient adminClient)
    {
        try
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(TimeoutWaitingForMetadataSeconds));
            return metadata.Brokers.Count > 0;
        }
        catch
        {
            return false;
        }
    }
}
