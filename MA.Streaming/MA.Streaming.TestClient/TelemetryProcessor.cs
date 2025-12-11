// <copyright file="TelemetryProcessor.cs" company="Motion Applied Ltd.">
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

using Google.Protobuf;

using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

namespace MA.Streaming.TestClient;

public class TelemetryProcessor
{
    private readonly string dataSource;
    private readonly string sessionKey;
    private readonly PacketReaderService.PacketReaderServiceClient packetReader;
    private readonly ConnectionManagerService.ConnectionManagerServiceClient connectionManager;
    private const string EngineStream = "engine";
    private const string BrakesStream = "brakes";
    private const string SuspensionStream = "suspension";
    private const string PositionStream = "position";

    public TelemetryProcessor(StreamingApiConfiguration configuration, string dataSource, string sessionKey)
    {
        this.dataSource = dataSource;
        this.sessionKey = sessionKey;
        StreamingApiClient.Initialise(
            configuration,
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(),
            new LoggingDirectoryProvider(@"C:\Temp"));

        this.packetReader = StreamingApiClient.GetPacketReaderClient();
        this.connectionManager = StreamingApiClient.GetConnectionManagerClient();
    }

    public async Task StartProcessing(CancellationToken cancellationToken = default)
    {
        // Create connection for all telemetry streams
        var connection = await this.connectionManager.NewConnectionAsync(
            new NewConnectionRequest
            {
                Details = new ConnectionDetails
                {
                    DataSource = this.dataSource,
                    SessionKey = this.sessionKey,
                    Streams =
                    {
                        EngineStream,
                        BrakesStream,
                        SuspensionStream,
                        PositionStream
                    },
                    StreamOffsets =
                    {
                        0,
                        0,
                        0,
                        0
                    } // Start from beginning
                }
            }, cancellationToken: cancellationToken);

        Console.WriteLine($"Connected for processing with ID: {connection.Connection.Id}");

        // Start processing stream
        var readRequest = new ReadPacketsRequest
        {
            Connection = connection.Connection
        };

        var stream = this.packetReader.ReadPackets(readRequest, cancellationToken: cancellationToken);

        while (await stream.ResponseStream.MoveNext(cancellationToken))
        {
            var response = stream.ResponseStream.Current;
            foreach (var data in response.Response)
            {
                await ProcessTelemetryPacket(data.Packet);
            }
        }
    }

    private static async Task ProcessTelemetryPacket(Packet packet)
    {
        try
        {
            // Process based on packet type
            switch (packet.Type)
            {
                case "PeriodicData":
                    var periodicData = PeriodicDataPacket.Parser.ParseFrom(packet.Content);
                    await ProcessPeriodicData(periodicData);
                    break;
                case "RowData":
                    var rowData = RowDataPacket.Parser.ParseFrom(packet.Content);
                    await ProcessRowData(rowData);
                    break;
                case "Event":
                    var eventData = EventPacket.Parser.ParseFrom(packet.Content);
                    await ProcessEventData(eventData);
                    break;
                default:
                    Console.WriteLine($"Unknown packet type: {packet.Type}");
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing packet from {packet.SessionKey}: {ex.Message}");
        }
        StreamingApiClient.GetSessionManagementClient();

    }

    private static async Task ProcessPeriodicData(PeriodicDataPacket periodicData)
    {
        // Extract parameter values based on the data format
        var parameterIds = periodicData.DataFormat.ParameterIdentifiers.ParameterIdentifiers ?? [];

        for (var columnIndex = 0; columnIndex < periodicData.Columns.Count; columnIndex++)
        {
            if (columnIndex >= parameterIds.Count)
            {
                break;
            }

            var parameterId = parameterIds[columnIndex];
            var column = periodicData.Columns[columnIndex];

            if (column.DoubleSamples != null)
            {
                foreach (var sample in column.DoubleSamples.Samples)
                {
                    await ProcessParameterValue(parameterId, sample.Value, sample.Status);
                }
            }
        }
    }

    private static async Task ProcessRowData(RowDataPacket rowData)
    {
        var parameterIds = rowData.DataFormat.ParameterIdentifiers?.ParameterIdentifiers ?? [];

        for (var rowIndex = 0; rowIndex < rowData.Rows.Count; rowIndex++)
        {
            var row = rowData.Rows[rowIndex];

            if (row.DoubleSamples != null)
            {
                for (var sampleIndex = 0; sampleIndex < row.DoubleSamples.Samples.Count && sampleIndex < parameterIds.Count; sampleIndex++)
                {
                    var parameterId = parameterIds[sampleIndex];
                    var sample = row.DoubleSamples.Samples[sampleIndex];

                    await ProcessParameterValue(parameterId, sample.Value, sample.Status);
                }
            }
        }
    }

    private static async Task ProcessParameterValue(string parameterId, double value, DataStatus status)
    {
        if (status != DataStatus.Valid)
        {
            Console.WriteLine($"Invalid data for {parameterId}: {status}");
            return;
        }

        // Process based on parameter identifier
        switch (parameterId)
        {
            case "Engine.Rpm":
                await ProcessEngineRpm(value);
                break;
            case "Engine.Temperature":
                await ProcessEngineTemperature(value);
                break;
            case "Engine.OilPressure":
                await ProcessOilPressure(value);
                break;
            case "Brakes.FrontLeft.Temperature":
            case "Brakes.FrontRight.Temperature":
            case "Brakes.RearLeft.Temperature":
            case "Brakes.RearRight.Temperature":
                await ProcessBrakeTemperature(parameterId, value);
                break;
            case "Position.Latitude":
            case "Position.Longitude":
            case "Position.Speed":
                await ProcessPositionData(parameterId, value);
                break;
            default:
                // Log unknown parameter for debugging
                Console.WriteLine($"Unknown parameter: {parameterId} = {value}");
                break;
        }
    }

    private static async Task ProcessEngineRpm(double rpm)
    {
        if (rpm > 6500)
        {
            await RaiseAlert("Engine Over-rev", $"Engine RPM: {rpm:F0}");
        }

        await UpdateDashboard(
            EngineStream,
            new
            {
                rpm
            });
    }

    private static async Task ProcessEngineTemperature(double temperature)
    {
        if (temperature > 95)
        {
            await RaiseAlert("Engine Overheat", $"Engine temperature: {temperature:F1}°C");
        }

        await UpdateDashboard(
            EngineStream,
            new
            {
                temperature
            });
    }

    private static async Task ProcessOilPressure(double pressure)
    {
        if (pressure < 2.0)
        {
            await RaiseAlert("Low Oil Pressure", $"Oil pressure: {pressure:F1} bar");
        }

        await UpdateDashboard(
            EngineStream,
            new
            {
                oil_pressure = pressure
            });
    }

    private static async Task ProcessBrakeTemperature(string location, double temperature)
    {
        if (temperature > 350)
        {
            await RaiseAlert("Brake Overheat", $"{location} temperature: {temperature:F1}°C");
        }

        await UpdateDashboard(
            BrakesStream,
            new
            {
                location,
                temperature
            });
    }

    private static async Task ProcessPositionData(string parameter, double value)
    {
        // Store position data for combined processing
        await UpdateDashboard(
            PositionStream,
            new
            {
                parameter,
                value
            });
    }

    private static async Task ProcessEventData(EventPacket eventData)
    {
        var eventId = eventData.DataFormat.EventIdentifier;
        var timestamp = eventData.Timestamp;
        var values = eventData.RawValues;

        switch (eventId)
        {
            case "Engine.OverTemp":
                var temperature = values.Count > 0 ? values[0] : 0;
                await RaiseAlert("Engine Overtemperature Event", $"Temperature: {temperature:F1}°C");
                break;
            case "Brakes.Overheat":
                var brakeLocation = values.Count > 1 ? $"Temperature: {values[1]:F1}°C" : "Unknown";
                await RaiseAlert("Brake Overheating Event", brakeLocation);
                break;
            default:
                Console.WriteLine($"Received event: {eventId} at {timestamp} with {values.Count} values");
                break;
        }
    }

    private static async Task RaiseAlert(string alertType, string message)
    {
        Console.WriteLine($"?? ALERT: {alertType} - {message}");

        // Send alert as an event packet
        var eventPacket = new EventPacket
        {
            DataFormat = new EventDataFormat
            {
                EventIdentifier = "System.Alert"
            },
            Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000000
        };

        var packetWriter = StreamingApiClient.GetPacketWriterClient();
        await packetWriter.WriteDataPacketAsync(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = "vehicle-telemetry",
                    Stream = "alerts",
                    SessionKey = "alert-system",
                    Message = new Packet
                    {
                        Content = eventPacket.ToByteString(),
                        Type = "Event",
                        IsEssential = true
                    }
                }
            });
    }

    private static async Task UpdateDashboard(string component, object data)
    {
        // Send to dashboard via WebSocket or similar
        await Task.Run(() =>
        {
            Console.WriteLine($"?? Dashboard Update: {component} = {data}");
        });
    }
}
