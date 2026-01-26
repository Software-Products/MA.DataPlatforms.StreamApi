// <copyright file="TestClient.cs" company="Motion Applied Ltd.">
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

using MA.Streaming.Abstraction;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;
using Microsoft.Extensions.Configuration;
using Serilog;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;

namespace MA.Streaming.TestClient;

[ExcludeFromCodeCoverage]
internal static class TestClient
{
    private static async Task Main(string[] args)
    {
        var port = IsPortAvailable(13579) ? 13579 : FindFreePortAfter(13579);

        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("Configs/AppConfig.json", optional: false, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();
        var serilogLogger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .Enrich.FromLogContext()
            .Enrich.WithProperty("Application", "StreamAPI")
            .CreateLogger();

        Log.Logger = serilogLogger;

        var logger = new SerilogLoggerAdapter(serilogLogger);

        var streamingApiConfiguration = new StreamingApiConfiguration(StreamCreationStrategy.TopicBased, "localhost:9094", [], port);
        var cancellationTokenSource = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cancellationTokenSource.Cancel();
            cancellationTokenSource.Dispose();
        };

        var streamApi = StreamingApiClientFactory.Create(
            streamingApiConfiguration,
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(),
            new LoggingDirectoryProvider(@"C:\Temp"),
            logger);
        await streamApi.InitialiseAsync();

        var tasks = new List<Task>();
        var producer = new TelemetryProducer(streamApi, "vehicle-001");

        tasks.Add(producer.StartCollecting(cancellationTokenSource.Token));

        var processor = new TelemetryProcessor(streamApi, producer.DataSource, producer.SessionKey);
        tasks.Add(processor.StartProcessing(cancellationTokenSource.Token));

        Console.WriteLine("Telemetry application started. Press Ctrl+C to stop.");

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Application stopped.");
        }
    }

    private static int FindFreePortAfter(int startPort, int maxPort = 65535)
    {
        for (var port = startPort + 1; port <= maxPort; port++)
        {
            if (IsPortAvailable(port))
            {
                return port;
            }
        }

        throw new InvalidOperationException("No free port found in the specified range.");
    }

    private static bool IsPortAvailable(int port)
    {
        try
        {
            using var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            listener.Stop();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }
}
