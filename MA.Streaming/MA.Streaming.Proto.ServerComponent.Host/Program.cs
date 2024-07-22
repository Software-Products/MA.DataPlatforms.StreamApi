// <copyright file="Program.cs" company="McLaren Applied Ltd.">
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

using System.Net;

namespace MA.Streaming.Proto.ServerComponent.Host;

internal static class Program
{
    private static void Main()
    {
        var autoResetEvent = new AutoResetEvent(false);

        AppDefinition.Load();

        Console.CancelKeyPress += (_, _) =>
        {
            Console.WriteLine("Stopping the application gracefully...");
            autoResetEvent.Set();
            Environment.Exit(0);
        };
        Console.WriteLine("Press Ctrl+C to stop the application.");

        if (!AppDefinition.AutoStart)
        {
            var listener = CreateListener(AppDefinition.ListenerListenerPortNumber);

            Console.WriteLine(
                $"environment variables -> port:{AppDefinition.ListenerListenerPortNumber} Config Path:{AppDefinition.StreamApiConfigFilePath}");

            Console.WriteLine("Listening for requests...");

            _ = Task.Run(
                async () =>
                {
                    while (true)
                    {
                        var context = await listener.GetContextAsync();
                        var request = context.Request;
                        var response = context.Response;
                        await RequestHandler.Handle(context, response, request);
                    }
                });
        }
        else
        {
            Console.WriteLine("Auto Start Enabled...");
            Console.WriteLine("Starting The Server...");
            ServerStarter.Start();
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
