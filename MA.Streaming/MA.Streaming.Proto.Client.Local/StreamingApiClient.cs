// <copyright file="StreamingApiClient.cs" company="Motion Applied Ltd.">
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

using System.Net.Security;
using System.Security.Authentication;

using Grpc.Net.Client;

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Proto.ServerComponent;

namespace MA.Streaming.Proto.Client.Local;

public class StreamingApiClient
{
    private readonly IStreamingApiConfiguration streamingApiConfiguration;
    private readonly ICancellationTokenSourceProvider cancellationTokenSourceProvider;
    private readonly IKafkaBrokerAvailabilityChecker kafkaBrokerAvailabilityChecker;
    private readonly ILoggingDirectoryProvider loggingDirectoryProvider;
    private readonly ILogger? logger;

    internal StreamingApiClient(
        IStreamingApiConfiguration streamingApiConfiguration,
        ICancellationTokenSourceProvider cancellationTokenSourceProvider,
        IKafkaBrokerAvailabilityChecker kafkaBrokerAvailabilityChecker,
        ILoggingDirectoryProvider loggingDirectoryProvider,
        ILogger? logger)
    {
        this.streamingApiConfiguration = streamingApiConfiguration;
        this.cancellationTokenSourceProvider = cancellationTokenSourceProvider;
        this.kafkaBrokerAvailabilityChecker = kafkaBrokerAvailabilityChecker;
        this.loggingDirectoryProvider = loggingDirectoryProvider;
        this.logger = logger;
        this.cancellationToken = cancellationTokenSourceProvider.Provide().Token;
    }

    private Server? server;
    private GrpcChannel? channel;

    public event EventHandler<DateTime>? ServiceInitialised;

    private readonly CancellationToken cancellationToken;
    private bool stopped;

    public bool Initialised { get; private set; }

    public void Initialise()
    {
        if (this.Initialised)
        {
            return;
        }

        try
        {
            this.server = new Server(
                this.streamingApiConfiguration,
                this.cancellationTokenSourceProvider,
                this.kafkaBrokerAvailabilityChecker,
                this.loggingDirectoryProvider,
                logger: this.logger);
            this.server.Start().Wait(TimeSpan.FromSeconds(10));
            this.SetupGrpcChannel();
        }
        catch (Exception ex)
        {
            if (this.logger != null)
            {
                this.logger.Error($"Error occurred while initializing StreamingApiClient. {ex}");
            }
            else
            {
                Console.WriteLine($"Error occurred while initializing StreamingApiClient. {ex} ");
            }

            this.Shutdown();
            throw;
        }

        this.Initialised = true;
        this.ServiceInitialised?.Invoke(null, DateTime.UtcNow);
    }

    public async Task InitialiseAsync()
    {
        try
        {
            if (this.Initialised)
            {
                return;
            }

            this.server = new Server(
                this.streamingApiConfiguration,
                this.cancellationTokenSourceProvider,
                this.kafkaBrokerAvailabilityChecker,
                this.loggingDirectoryProvider);
            await this.server.StartAsync(this.cancellationToken);
            this.SetupGrpcChannel();
            this.Initialised = true;
            this.ServiceInitialised?.Invoke(null, DateTime.UtcNow);
        }
        catch (OperationCanceledException)
        {
            this.Shutdown();
        }

        catch (Exception ex)
        {
            if (this.logger != null)
            {
                this.logger.Error($"Error occurred while initializing StreamingApiClient. {ex}");
            }
            else
            {
                Console.WriteLine($"Error occurred while initializing StreamingApiClient. {ex} ");
            }

            this.Shutdown();
        }
    }

    public object? GetService(Type serviceType)
    {
        return this.server?.GetService(serviceType);
    }

    public void Shutdown()
    {
        if (this.stopped)
        {
            return;
        }

        this.server?.Stop().Wait(this.cancellationToken);
        this.channel?.ShutdownAsync().Wait(this.cancellationToken);
        this.channel?.Dispose();
        this.channel = null;
        this.server?.Dispose();
        this.server = null;
        this.Initialised = false;
        this.stopped = true;
    }

    public ConnectionManagerService.ConnectionManagerServiceClient GetConnectionManagerClient()
    {
        this.CheckInitialised();
        return new ConnectionManagerService.ConnectionManagerServiceClient(this.channel);
    }

    public DataFormatManagerService.DataFormatManagerServiceClient GetDataFormatManagerClient()
    {
        this.CheckInitialised();

        return new DataFormatManagerService.DataFormatManagerServiceClient(this.channel);
    }

    public PacketReaderService.PacketReaderServiceClient GetPacketReaderClient()
    {
        this.CheckInitialised();

        return new PacketReaderService.PacketReaderServiceClient(this.channel);
    }

    public PacketWriterService.PacketWriterServiceClient GetPacketWriterClient()
    {
        this.CheckInitialised();

        return new PacketWriterService.PacketWriterServiceClient(this.channel);
    }

    public SessionManagementService.SessionManagementServiceClient GetSessionManagementClient()
    {
        this.CheckInitialised();

        return new SessionManagementService.SessionManagementServiceClient(this.channel);
    }

    private void CheckInitialised()
    {
        if (!this.Initialised)
        {
            throw new InvalidOperationException(
                "StreamApi StreamingApiClient has not been initialised. Please call StreamingApiClient.Initialise() or StreamingApiClient.InitialiseAsync() before using the API.");
        }
    }

    private void SetupGrpcChannel()
    {
        this.channel = GrpcChannel.ForAddress(
            $"http://localhost:{this.streamingApiConfiguration.StreamApiPort}",
            new GrpcChannelOptions
            {
                HttpHandler = new SocketsHttpHandler
                {
                    SslOptions = new SslClientAuthenticationOptions
                    {
                        EnabledSslProtocols = SslProtocols.None
                    }
                }
            });
    }
}
