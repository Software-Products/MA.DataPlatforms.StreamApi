// <copyright file="RemoteStreamingApiClient.cs" company="McLaren Applied Ltd.">
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

using System.Net.Security;
using System.Security.Authentication;

using Grpc.Net.Client;

using MA.Streaming.API;

namespace MA.Streaming.Proto.Client.Remote;

public static class RemoteStreamingApiClient
{
    private static GrpcChannel? channel;

    public static bool Initialised { get; private set; }

    public static void Initialise(string serverAddress)
    {
        if (Initialised)
        {
            return;
        }

        try
        {
            channel = GrpcChannel.ForAddress(
                $"http://{serverAddress}",
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
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            throw;
        }

        Initialised = true;
    }

    public static void Shutdown()
    {
        channel?.Dispose();
        channel = null;
        Initialised = false;
    }

    public static ConnectionManagerService.ConnectionManagerServiceClient GetConnectionManagerClient()
    {
        CheckInitialised();
        return new ConnectionManagerService.ConnectionManagerServiceClient(channel);
    }

    public static DataFormatManagerService.DataFormatManagerServiceClient GetDataFormatManagerClient()
    {
        CheckInitialised();

        return new DataFormatManagerService.DataFormatManagerServiceClient(channel);
    }

    public static PacketReaderService.PacketReaderServiceClient GetPacketReaderClient()
    {
        CheckInitialised();

        return new PacketReaderService.PacketReaderServiceClient(channel);
    }

    public static PacketWriterService.PacketWriterServiceClient GetPacketWriterClient()
    {
        CheckInitialised();

        return new PacketWriterService.PacketWriterServiceClient(channel);
    }

    public static SessionManagementService.SessionManagementServiceClient GetSessionManagementClient()
    {
        CheckInitialised();

        return new SessionManagementService.SessionManagementServiceClient(channel);
    }

    private static void CheckInitialised()
    {
        if (!Initialised)
        {
            throw new InvalidOperationException(
                "StreamApi StreamingApiClient has not been initialised. Please call StreamingApiClient.Initialise() before using the API.");
        }
    }
}
