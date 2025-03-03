// <copyright file="Server.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Proto.Core.Abstractions;
using MA.Streaming.Proto.Core.Services;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using ILogger = MA.Common.Abstractions.ILogger;

namespace MA.Streaming.Proto.ServerComponent;

public sealed class Server : IServer
{
    private const int ExpectedUrlCount = 1; // HTTP 
    private readonly IStreamingApiConfiguration streamingApiConfiguration;
    private readonly ICancellationTokenSourceProvider cancellationTokenSourceProvider;
    private readonly IKafkaBrokerAvailabilityChecker kafkaBrokerAvailabilityChecker;
    private readonly ILoggingDirectoryProvider loggingDirectoryProvider;
    private readonly bool registerServices;
    private readonly object locker = new();
    private WebApplication? webApp;

    private CancellationTokenSource? availabilityCheckerTokenSource;

    public Server(
        IStreamingApiConfiguration streamingApiConfiguration,
        ICancellationTokenSourceProvider cancellationTokenSourceProvider,
        IKafkaBrokerAvailabilityChecker kafkaBrokerAvailabilityChecker,
        ILoggingDirectoryProvider loggingDirectoryProvider,
        bool registerServices = false)
    {
        this.streamingApiConfiguration = streamingApiConfiguration;
        this.cancellationTokenSourceProvider = cancellationTokenSourceProvider;
        this.kafkaBrokerAvailabilityChecker = kafkaBrokerAvailabilityChecker;
        this.loggingDirectoryProvider = loggingDirectoryProvider;
        this.registerServices = registerServices;
    }

    public void Dispose()
    {
        this.Stop().Wait();
    }

    public object? GetService(Type serviceType)
    {
        return this.webApp?.Services.GetService(serviceType);
    }

    public Task Start()
    {
        return this.StartInternal().ContinueWith(
            tsk => tsk.IsCompletedSuccessfully && this.webApp?.Urls.Count == ExpectedUrlCount
                ? Task.CompletedTask
                : throw tsk.Exception ?? new Exception("Start server failed"));
    }

    public Task Stop()
    {
        this.availabilityCheckerTokenSource?.Cancel();

        WebApplication? webAppCopy;
        lock (this.locker)
        {
            webAppCopy = this.webApp;
            this.webApp = null;
        }

        return webAppCopy?.DisposeAsync().AsTask() ?? Task.CompletedTask;
    }

    private Task StartInternal()
    {
        // Create the web app builder
        // This currently uses the port defined in the configuration the default endpoints of http://localhost:5000 
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(
            this.ConfigureListenOptions
        );

        builder.WebHost.ConfigureLogging(
            logging =>
            {
                logging.ClearProviders();
                logging.SetMinimumLevel(LogLevel.Warning);
                logging.AddConsole();
            });

        builder.Services.AddGrpc();
       
        if (this.registerServices)
        {
            builder.Services.AddTransient<ConnectionManager>();
            builder.Services.AddTransient<DataFormatManager>();
            builder.Services.AddTransient<PacketWriter>();
            builder.Services.AddTransient<PacketReader>();
            builder.Services.AddTransient<SessionManager>();
        }

        // Add services to the container.
        new ServiceConfigurator(this.streamingApiConfiguration, this.cancellationTokenSourceProvider, this.loggingDirectoryProvider).Configure(builder.Services);

        this.webApp = builder.Build();

        // Configure the HTTP request pipeline.
        this.webApp.MapGrpcService<PacketWriter>();
        this.webApp.MapGrpcService<PacketReader>();
        this.webApp.MapGrpcService<ConnectionManager>();
        this.webApp.MapGrpcService<DataFormatManager>();
        this.webApp.MapGrpcService<SessionManager>();
        this.webApp.MapGet(
            "/",
            () =>
                "Communication with this gRPC endpoint must be made through a Stream API client.");

        var logger = this.webApp.Services.GetService<ILogger>();
        var kafkaAvailable = false;
        while (!kafkaAvailable)
        {
            if (this.kafkaBrokerAvailabilityChecker.Check(this.streamingApiConfiguration.BrokerUrl))
            {
                kafkaAvailable = true;
                continue;
            }

            logger?.Error("Kafka is not available will retry in 5 seconds");
            Task.Delay(TimeSpan.FromSeconds(5)).Wait();
        }

        if (this.streamingApiConfiguration.IntegrateDataFormatManagement)
        {
            this.webApp.Services.GetService<IDataFormatInfoService>()?.Start();
        }

        if (!this.streamingApiConfiguration.IntegrateSessionManagement)
        {
            return this.webApp.StartAsync();
        }

        this.webApp.Services.GetService<ISessionInfoService>()?.Start();
        this.webApp.Services.GetService<ISessionNotificationManagerService>()?.Start();

        return this.webApp.StartAsync();
    }

    private void ConfigureListenOptions(KestrelServerOptions options)
    {
        options.ListenAnyIP(
            this.streamingApiConfiguration.StreamApiPort,
            listenOption =>
            {
                listenOption.Protocols = HttpProtocols.Http2;
            });
    }
}
