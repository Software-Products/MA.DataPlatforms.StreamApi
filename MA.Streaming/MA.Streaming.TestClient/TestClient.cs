// <copyright file="TestClient.cs" company="McLaren Applied Ltd.">
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

using MA.Streaming.API;
using MA.Streaming.Proto.Client.Remote;

namespace MA.Streaming.TestClient;

internal static class TestClient
{
    public static void Main(string[] args)
    {
        RemoteStreamingApiClient.Initialise("localhost:9092");

        Console.WriteLine("Initialised StreamingApiClient");

        TestConnection();

        RemoteStreamingApiClient.Shutdown();

        Console.ReadLine();

        Console.WriteLine("Shutdown StreamingApiClient");
    }

    private static void TestConnection()
    {
        var cmc = RemoteStreamingApiClient.GetConnectionManagerClient();

        var newRequest = new NewConnectionRequest
        {
            Details = new ConnectionDetails
            {
                DataSource = "TestDataSource"
            }
        };
        var connectionResponse = cmc.NewConnection(newRequest);

        Console.WriteLine($"Connection established with Id {connectionResponse.Connection.Id}");

        var getResponse = cmc.GetConnection(
            new GetConnectionRequest
            {
                Connection = new Connection
                {
                    Id = connectionResponse.Connection.Id
                }
            });

        Console.WriteLine(getResponse.Details != null ? "Connection verified" : "ERROR - connection not found");

        var closeResponse = cmc.CloseConnection(
            new CloseConnectionRequest
            {
                Connection = new Connection
                {
                    Id = connectionResponse.Connection.Id
                }
            });

        Console.WriteLine(closeResponse.Success ? "Connection closed" : "ERROR - connection not closed");
    }
}
