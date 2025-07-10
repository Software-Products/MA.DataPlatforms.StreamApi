// <copyright file="ServerStarter.cs" company="McLaren Applied Ltd.">
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

using System.Text.Json;

using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;

namespace MA.Streaming.Proto.ServerComponent.Host;

internal static class ServerStarter
{
    private static bool started;
    private static readonly object HandleRequestLock = new();

    public static void Start()
    {
        try
        {
            lock (HandleRequestLock)
            {
                if (started)
                {
                    return;
                }

                var hostConfig = JsonSerializer.Deserialize<HostStreamApiConfiguration>(
                    File.ReadAllText(Environment.CurrentDirectory + AppDefinition.StreamApiConfigFilePath));

                if (hostConfig == null)
                {
                    Console.WriteLine("invalid config file. app will exit");
                    return;
                }

                var streamApiConfig = new StreamingApiConfiguration(
                    hostConfig.StreamCreationStrategy,
                    hostConfig.BrokerUrl,
                    hostConfig.PartitionMappings,
                    hostConfig.StreamApiPort,
                    hostConfig.IntegrateSessionManagement,
                    hostConfig.IntegrateDataFormatManagement,
                    hostConfig.UseRemoteKeyGenerator,
                    hostConfig.RemoteKeyGeneratorServiceAddress,
                    hostConfig.BatchingResponses,
                    hostConfig.InitialisationTimeoutSeconds);

                var server = new Server(streamApiConfig, new CancellationTokenSourceProvider(), new KafkaBrokerAvailabilityChecker(), new LoggingDirectoryProvider(""));
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await server.Start();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                        Environment.Exit(-1);
                    }
                });

                Console.WriteLine($"Stream Api Server Started on port {streamApiConfig.StreamApiPort}...");
                started = true;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            throw;
        }
    }
}
