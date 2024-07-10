// <copyright file="RequestHandler.cs" company="McLaren Applied Ltd.">
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
using System.Text;
using System.Text.Json;

using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;

namespace MA.Streaming.Proto.ServerComponent.Host;

internal static class RequestHandler
{
    private static bool started;
    private static readonly object HandleRequestLock = new();

    public static Task Handle(
        HttpListenerContext context,
        HttpListenerResponse response,
        HttpListenerRequest request)
    {
        if (request.HttpMethod == "OPTIONS")
        {
            response.Headers.Add("Access-Control-Allow-Headers", "Content-Type, Accept, X-Requested-With");
            response.Headers.Add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
            response.Headers.Add("Access-Control-Allow-Origin", "*");
            response.Headers.Add("Access-Control-Max-Age", "1728000");
            response.StatusCode = (int)HttpStatusCode.NoContent;
            response.Close();
            return Task.CompletedTask;
        }

        var switchKey = context.Request.Url != null
            ? context.Request.Url.AbsolutePath.TrimEnd().TrimEnd('/').TrimStart('/')
            : "notValid";
        Console.WriteLine($"{DateTime.Now.TimeOfDay}:handle request {context.Request.Url} and switch is {switchKey}");
        switch (switchKey)
        {
            case "start":
                HandleStartServer(response);
                break;
            default:
                response.StatusCode = (int)HttpStatusCode.NotFound;
                response.Close();
                break;
        }

        return Task.CompletedTask;
    }

    private static void HandleStartServer(HttpListenerResponse response)
    {
        try
        {
            lock (HandleRequestLock)
            {
                if (started)
                {
                    response.OutputStream.Write(Encoding.UTF8.GetBytes("stream api server has been started..."));
                    response.Close();
                    return;
                }

                var config = JsonSerializer.Deserialize<StreamingApiConfiguration>(
                    File.ReadAllText(Environment.CurrentDirectory + AppDefinition.StreamApiConfigFilePath));

                if (config == null)
                {
                    LogExceptionAndReturnInternalServerError("invalid config file. app will exit", response);
                    return;
                }

                var server = new Server(config, new CancellationTokenSourceProvider(), new KafkaBrokerAvailabilityChecker());
                _ = Task.Run(async () => await server.Start());

                Console.WriteLine($"Stream Api Server Started on port {config.StreamApiPort}...");
                SendSuccessMessage(response, config);
                started = true;
            }
        }
        catch (Exception ex)
        {
            LogExceptionAndReturnInternalServerError(ex.ToString(), response);
            throw;
        }
    }

    private static void LogExceptionAndReturnInternalServerError(string logMessage, HttpListenerResponse response)
    {
        Console.WriteLine(logMessage);
        response.StatusCode = (int)HttpStatusCode.InternalServerError;
        response.Close();
    }

    private static void SendSuccessMessage(HttpListenerResponse response, StreamingApiConfiguration config)
    {
        response.StatusCode = (int)HttpStatusCode.OK;
        response.Headers.Add("Access-Control-Allow-Origin", "*");
        response.ContentType = "text/plain";
        response.ContentEncoding = Encoding.UTF8;
        var buffer = Encoding.UTF8.GetBytes($"Stream Api Started Properly and Listen To Requests On Port:{config.StreamApiPort}");
        response.ContentLength64 = buffer.Length;
        response.OutputStream.Write(buffer);
        response.Close();
    }
}
