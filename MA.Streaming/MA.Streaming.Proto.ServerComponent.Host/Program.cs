// <copyright file="Program.cs" company="Motion Applied Ltd.">
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

using Prometheus;

namespace MA.Streaming.Proto.ServerComponent.Host;

[ExcludeFromCodeCoverage]
internal static class Program
{
    private static readonly CancellationTokenSource CancellationTokenSource = new();

    private static void Main()
    {
        var autoResetEvent = new AutoResetEvent(false);

        AppDefinition.Load();

        Console.CancelKeyPress += (_, _) =>
        {
            Console.WriteLine("Stopping the application gracefully...");
            autoResetEvent.Set();
            Environment.Exit(0);
            CancellationTokenSource.Cancel();
            CancellationTokenSource.Dispose();
        };
        Console.WriteLine("Press Ctrl+C to stop the application.");

        if (!AppDefinition.AutoStart)
        {
            var listener = CreateListener(AppDefinition.ListenerPortNumber);

            Console.WriteLine(
                $"environment variables -> port:{AppDefinition.ListenerPortNumber} Config Path:{AppDefinition.StreamApiConfigFilePath}");

            Console.WriteLine("Listening for requests...");

            _ = Task.Run(
                async () =>
                {
                    while (!CancellationTokenSource.Token.IsCancellationRequested)
                    {
                        var context = await listener.GetContextAsync();
                        var request = context.Request;
                        var response = context.Response;
                        await RequestHandler.Handle(context, response, request);
                    }
                },
                CancellationTokenSource.Token);
        }
        else
        {
            Console.WriteLine("Auto Start Enabled...");
            Console.WriteLine("Starting The Server...");
            ServerStarter.Start();
        }

        try
        {
            if (!int.TryParse(AppDefinition.PrometheusMetricPort, out var promPort))
            {
                promPort = 10010;
            }

            var metricServer = new MetricServer(promPort);
            metricServer.Start();
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
        }

        autoResetEvent.WaitOne();
    }

    private static HttpListener CreateListener(string portNumber)
    {
        var root = $"http://*:{portNumber}";
        var baseUrl = $"{root}/";
        var start = $"{root}/start/";

        var listener = new HttpListener();
        listener.Prefixes.Add(baseUrl);
        listener.Prefixes.Add(start);
        listener.Start();
        return listener;
    }
}
