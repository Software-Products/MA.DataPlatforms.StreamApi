// <copyright file="KafkaBrokerAvailabilityChecker.cs" company="Motion Applied Ltd.">
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

using MA.Common;
using MA.Common.Abstractions;
using MA.Streaming.Abstraction;

namespace MA.Streaming.Proto.ServerComponent;

public class KafkaBrokerAvailabilityChecker : IKafkaBrokerAvailabilityChecker
{
    private const double TimeoutWaitingForMetadataSeconds = 2;
    private const double DefaultTimeBetweenRetriesSeconds = 5;

    private readonly Func<string, IAdminClient> adminClientFactory;
    private readonly double timeBetweenRetriesSeconds;

    public KafkaBrokerAvailabilityChecker()
        : this(
            brokerUrl => new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokerUrl }).Build())
    {
    }

    internal KafkaBrokerAvailabilityChecker(Func<string, IAdminClient> adminClientFactory, double timeBetweenRetriesSeconds = DefaultTimeBetweenRetriesSeconds)
    {
        this.adminClientFactory = adminClientFactory;
        this.timeBetweenRetriesSeconds = timeBetweenRetriesSeconds;
    }

    public bool Check(string brokerUrl, ILogger? logger = null)
    {
        logger ??= new ConsoleLogger();
        using var adminClient = this.adminClientFactory(brokerUrl);
        var brokerAvailable = IsBrokerAvailable(adminClient);

        if (brokerAvailable)
        {
            logger.Info("Kafka broker is available. Proceeding with the rest of the code...");
        }
        else
        {
            logger.Warning("Kafka broker is not available");
        }

        return brokerAvailable;
    }

    public Task CheckContinuously(string brokerUrl, CancellationTokenSource cancellationTokenSource, ILogger? logger = null)
    {
        return Task.Run(
            async () =>
            {
                logger ??= new ConsoleLogger();
                using var adminClient = this.adminClientFactory(brokerUrl);
                var wasAvailable = IsBrokerAvailable(adminClient);

                while (!cancellationTokenSource.IsCancellationRequested)
                {
                    var isAvailable = IsBrokerAvailable(adminClient);

                    if (isAvailable && !wasAvailable)
                    {
                        logger.Info("Kafka broker is available again");
                    }

                    var stateChanged = isAvailable != wasAvailable;
                    wasAvailable = isAvailable;

                    if (!stateChanged && !cancellationTokenSource.IsCancellationRequested)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(this.timeBetweenRetriesSeconds), cancellationTokenSource.Token);
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
