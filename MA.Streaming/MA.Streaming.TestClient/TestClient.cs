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

using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;

using MA.Streaming.Abstraction;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

using Microsoft.Extensions.Configuration;

using Serilog;
using Serilog.Core;

namespace MA.Streaming.TestClient;

[ExcludeFromCodeCoverage]
internal static class TestClient
{
    private static async Task Main(string[] args)
    {
        var port = IsPortAvailable(13579) ? 13579 : FindFreePortAfter(13579);

        // -- suppress ASP.NET Core / Kestrel console noise ---------------------
        // Must be set BEFORE StreamingApiClientFactory creates the Kestrel host,
        // because the embedded host reads these environment variables at startup.
        // Serilog alone is not enough: ASP.NET Core's ILoggerFactory writes its
        // own request/routing/hosting Info lines directly to the console,
        // completely independently of any Serilog configuration we provide.
        //
        // Default: Info-level ASP.NET Core / gRPC framework logs are suppressed.
        // Pass --verbose to restore them (useful when debugging the library itself).
        var threadLeakMode = args.Contains("--thread-leak");
        var verbose = args.Contains("--verbose");

        if (!verbose)
        {
            const string Warning = "Warning";
            Environment.SetEnvironmentVariable("Logging__LogLevel__Default", Warning);
            Environment.SetEnvironmentVariable("Logging__LogLevel__Microsoft", Warning);
            Environment.SetEnvironmentVariable("Logging__LogLevel__Microsoft.AspNetCore", Warning);
            Environment.SetEnvironmentVariable("Logging__LogLevel__Microsoft.AspNetCore.Hosting", Warning);
            Environment.SetEnvironmentVariable("Logging__LogLevel__Microsoft.AspNetCore.Routing", Warning);
            Environment.SetEnvironmentVariable("Logging__LogLevel__Grpc", Warning);
        }

        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("Configs/AppConfig.json", optional: false, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

        // Serilog logger selection:
        //   --verbose              ? full config from AppConfig.json (all levels)
        //   --thread-leak          ? Warning+ only (clean stats-only output)
        //   default (no flags)     ? Warning+ only (clean application output)
        Logger serilogLogger;
        if (verbose)
        {
            serilogLogger = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                .Enrich.FromLogContext()
                .Enrich.WithProperty("Application", "StreamAPI")
                .CreateLogger();
        }
        else
        {
            serilogLogger = new LoggerConfiguration()
                .MinimumLevel.Warning()
                .WriteTo.Console(outputTemplate: "[{Level:u3}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger();
        }

        Log.Logger = serilogLogger;

        var logger = new SerilogLoggerAdapter(serilogLogger);

        var streamingApiConfiguration = new StreamingApiConfiguration(
            StreamCreationStrategy.TopicBased,
            "localhost:9094",
            [],
            port,
            integrateSessionManagement: true);
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

        // -- --monitor mode ----------------------------------------------------
        // Usage:
        //   dotnet run -- --monitor [--data-source <DataSource>]
        //
        // DataSource is resolved in this order:
        //   1. --data-source <value>  (CLI override)
        //   2. MonitorConfig:DataSource in AppConfig.json
        //
        // Listens for live session start/stop notifications, reads every packet
        // across ALL streams, and prints per-type packet counts + thread count
        // every 5 seconds.
        if (args.Contains("--monitor"))
        {
            var dataSource = GetArgString(args, "--data-source")
                ?? configuration["MonitorConfig:DataSource"];

            if (string.IsNullOrEmpty(dataSource))
            {
                Console.Error.WriteLine("ERROR: --monitor requires --data-source <DataSource> or MonitorConfig.DataSource in AppConfig.json");
                return;
            }

            var monitor = new LiveSessionMonitor(streamApi, dataSource);

            try
            {
                await monitor.RunAsync(cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Monitor stopped.");
            }

            return;
        }

        // -- --thread-leak mode ------------------------------------------------
        // Usage:
        //   dotnet run -- --thread-leak [--sessions <N>] [--duration-seconds <S>]
        //
        // Defaults: 3 concurrent sessions, each lasting 60 seconds.
        if (threadLeakMode)
        {
            var sessions = GetArgInt(args, "--sessions", defaultValue: 3);
            var duration = GetArgInt(args, "--duration-seconds", defaultValue: 60);

            Console.WriteLine($"Thread-leak scenario  sessions={sessions}  duration={duration}s");
            Console.WriteLine();

            var scenario = new ThreadLeakScenario(streamApi, sessions, duration);

            try
            {
                await scenario.RunAsync(cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Scenario cancelled.");
            }

            return;
        }

        // -- default: live producer + processor --------------------------------
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

    /// <summary>Parses a string value from a named argument pair (--key value).</summary>
    private static string? GetArgString(string[] args, string key)
    {
        var idx = Array.IndexOf(args, key);
        return idx >= 0 && idx + 1 < args.Length ? args[idx + 1] : null;
    }

    /// <summary>Parses an integer value from a named argument pair (--key value).</summary>
    private static int GetArgInt(string[] args, string key, int defaultValue)
    {
        var idx = Array.IndexOf(args, key);
        if (idx >= 0 &&
            idx + 1 < args.Length &&
            int.TryParse(args[idx + 1], out var value))
        {
            return value;
        }

        return defaultValue;
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
