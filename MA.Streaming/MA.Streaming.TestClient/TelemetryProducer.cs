// <copyright file="TelemetryProducer.cs" company="Motion Applied Ltd.">
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
using Google.Protobuf.WellKnownTypes;

using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

namespace MA.Streaming.TestClient;

public class TelemetryProducer
{
    private readonly PacketWriterService.PacketWriterServiceClient packetWriter;
    public readonly string SessionKey;
    private readonly string vehicleId;
    private const string AppName = "VehicleTelemetry";
    private const string EngineGroup = "Engine";
    private const string BrakesGroup = "Brakes";
    private const string PositionGroup = "Position";
    private const string DefaultFormatString = "%5.1f";
    // Create configuration packet with all telemetry parameters
    private readonly ConfigurationPacket telemetryConfig = new()
    {
        ConfigId = "vehicle-telemetry-v1",
        ParameterDefinitions =
        {
            // Engine parameters
            new ParameterDefinition
            {
                Identifier = "Engine.Rpm",
                Name = "Engine RPM",
                ApplicationName = AppName,
                Description = "Engine revolutions per minute",
                Units = "RPM",
                DataType = DataType.Float64,
                FormatString = "%5.0f",
                MinValue = 0.0,
                MaxValue = 12000.0,
                Groups = { EngineGroup },
                Frequencies =
                {
                    10.0
                }, // 10 Hz
                IncludesRowData = true
            },
            new ParameterDefinition
            {
                Identifier = "Engine.Temperature",
                Name = "Engine Temperature",
                ApplicationName = AppName,
                Description = "Engine coolant temperature",
                Units = "°C",
                DataType = DataType.Float64,
                FormatString = DefaultFormatString,
                Groups = { EngineGroup },
                MinValue = -40.0,
                MaxValue = 150.0,
                Frequencies =
                {
                    1.0
                } // 1 Hz
            },
            new ParameterDefinition
            {
                Identifier = "Engine.OilPressure",
                Name = "Oil Pressure",
                ApplicationName = AppName,
                Description = "Engine oil pressure",
                Units = "bar",
                DataType = DataType.Float64,
                FormatString = DefaultFormatString,
                Groups = { EngineGroup },
                MinValue = 0.0,
                MaxValue = 10.0,
                Frequencies =
                {
                    2.0
                } // 2 Hz
            },
            // Brake parameters
            new ParameterDefinition
            {
                Identifier = "Brakes.FrontLeft.Temperature",
                Name = "Front Left Brake Temperature",
                ApplicationName = AppName,
                Description = "Front left brake disc temperature",
                Units = "°C",
                DataType = DataType.Float64,
                FormatString = DefaultFormatString,
                MinValue = 0.0,
                MaxValue = 800.0,
                Groups = { BrakesGroup },
                WarningMaxValue = 700.0,
                Frequencies =
                {
                    5.0
                } // 5 Hz
            },
            // Position parameters
            new ParameterDefinition
            {
                Identifier = "Position.Latitude",
                Name = "Latitude",
                ApplicationName = AppName,
                Description = "GPS latitude coordinate",
                Units = "degrees",
                DataType = DataType.Float64,
                FormatString = "%5.6f",
                Groups = { PositionGroup },
                MinValue = -90.0,
                MaxValue = 90.0,
                IncludesRowData = true
            },
            new ParameterDefinition
            {
                Identifier = "Position.Longitude",
                Name = "Longitude",
                ApplicationName = AppName,
                Description = "GPS longitude coordinate",
                Units = "degrees",
                DataType = DataType.Float64,
                FormatString = "%5.6f",
                Groups = { PositionGroup },
                MinValue = -180.0,
                MaxValue = 180.0,
                IncludesRowData = true
            },
            new ParameterDefinition
            {
                Identifier = "Position.Speed",
                Name = "Vehicle Speed",
                ApplicationName = AppName,
                Description = "Vehicle ground speed",
                Units = "km/h",
                DataType = DataType.Float64,
                FormatString = DefaultFormatString,
                MinValue = 0.0,
                MaxValue = 400.0,
                Groups = { PositionGroup },
                IncludesRowData = true
            }
        },
        EventDefinitions =
        {
            new EventDefinition
            {
                Identifier = "Engine.OverTemp",
                Name = "Engine Overtemperature",
                ApplicationName = AppName,
                Description = "Engine temperature exceeds safe operating limits",
                Priority = EventPriority.Critical,
                Groups = { EngineGroup },
                DataTypes =
                {
                    DataType.Float64
                }, // Temperature value
                Units =
                {
                    "°C"
                }
            },
            new EventDefinition
            {
                Identifier = "Brakes.Overheat",
                Name = "Brake Overheating",
                ApplicationName =AppName,
                Description = "Brake temperature exceeds safe limits",
                Priority = EventPriority.High,
                Groups = { BrakesGroup },
                DataTypes =
                {
                    DataType.String,
                    DataType.Float64
                }, // Location, Temperature
                Units =
                {
                    "",
                    "°C"
                }
            }
        },
        GroupDefinitions =
        {
            new GroupDefinition
            {
                Identifier = AppName,
                ApplicationName = AppName,
                Name = AppName,
                Description = "Vehicle Telemetry",
                Groups =
                {
                    new GroupDefinition
                    {
                        Identifier = BrakesGroup,
                        ApplicationName = AppName,
                        Name = BrakesGroup,
                        Description = "Brake system telemetry"
                    },
                    new GroupDefinition
                    {
                        Identifier = EngineGroup,
                        ApplicationName = AppName,
                        Name = EngineGroup,
                        Description = "Engine telemetry"
                    },
                    new GroupDefinition
                    {
                        Identifier = PositionGroup,
                        ApplicationName = AppName,
                        Name = PositionGroup,
                        Description = "Vehicle position telemetry"
                    }
                }
            }
        }
    };
    public readonly string DataSource;

    public TelemetryProducer(StreamingApiConfiguration configuration, string vehicleId)
    {
        StreamingApiClient.Initialise(
            configuration,
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(),
            new LoggingDirectoryProvider(@"C:\Temp"));
        var sessionManagement = StreamingApiClient.GetSessionManagementClient();
        var response = sessionManagement.CreateSession(
            new CreateSessionRequest
            {
                DataSource = vehicleId,
                Type = "Session",
                Version = 1,
                UtcOffset = new Duration
                {
                    Seconds = 0
                },
                Identifier = "sample-session",
                Details =
                {
                    {
                        "Description", $"Telemetry session for vehicle {vehicleId}"
                    },
                    {
                        "weather", "dry"
                    },
                    {
                        "temperature", "22°C"
                    }
                }
            });

        Console.WriteLine($"Session {response.SessionKey} created.");
        this.packetWriter = StreamingApiClient.GetPacketWriterClient();
        this.vehicleId = vehicleId;
        this.SessionKey = response.SessionKey;
        this.DataSource = vehicleId;
    }

    public async Task StartCollecting(CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"Starting telemetry collection for vehicle {this.vehicleId}");

        // Send configuration first
        await this.SendConfiguration();

        while (!cancellationToken.IsCancellationRequested)
        {
            var timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000000; // Convert to nanoseconds

            // Send telemetry data as periodic packets
            await Task.WhenAll(
                this.StreamEngineData(timestamp),
                this.StreamPositionData(timestamp),
                this.CheckForEvents(timestamp)
            );

            await Task.Delay(100, cancellationToken); // 10Hz data rate
        }
    }

    private async Task SendConfiguration()
    {
        // Send the telemetry configuration (as defined above)
        await this.packetWriter.WriteDataPacketAsync(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    SessionKey = this.SessionKey,
                    Message = new Packet
                    {
                        Content = this.telemetryConfig.ToByteString(),
                        Type = "Configuration",
                        IsEssential = true,
                        SessionKey = this.SessionKey
                    }
                }
            });
    }

    private async Task StreamEngineData(ulong timestamp)
    {
        // Simulate engine sensor readings
        var engineData = new PeriodicDataPacket
        {
            DataFormat = new SampleDataFormat
            {
                ParameterIdentifiers = new ParameterList
                {
                    ParameterIdentifiers =
                    {
                        "Engine.Rpm",
                        "Engine.Temperature",
                        "Engine.OilPressure"
                    }
                }
            },
            StartTime = timestamp,
            Interval = 100000000, // 100ms in nanoseconds  
            Columns =
            {
                new SampleColumn // RPM
                {
                    DoubleSamples = new DoubleSampleList
                    {
                        Samples =
                        {
                            new DoubleSample
                            {
                                Value = GenerateRpm(),
                                Status = DataStatus.Valid
                            }
                        }
                    }
                },
                new SampleColumn // Temperature
                {
                    DoubleSamples = new DoubleSampleList
                    {
                        Samples =
                        {
                            new DoubleSample
                            {
                                Value = GenerateEngineTemp(),
                                Status = DataStatus.Valid
                            }
                        }
                    }
                },
                new SampleColumn // Oil Pressure
                {
                    DoubleSamples = new DoubleSampleList
                    {
                        Samples =
                        {
                            new DoubleSample
                            {
                                Value =     GenerateOilPressure(),
                                Status = DataStatus.Valid
                            }
                        }
                    }
                }
            }
        };

        await this.packetWriter.WriteDataPacketAsync(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = "engine",
                    SessionKey = this.SessionKey,
                    Message = new Packet
                    {
                        Content = engineData.ToByteString(),
                        Type = "PeriodicData",
                        IsEssential = false,
                        SessionKey = this.SessionKey
                    }
                }
            });
    }

    private async Task StreamPositionData(ulong timestamp)
    {
        // GPS position as row data (timestamped samples)
        var positionData = new RowDataPacket
        {
            DataFormat = new SampleDataFormat
            {
                ParameterIdentifiers = new ParameterList
                {
                    ParameterIdentifiers =
                    {
                        "Position.Latitude",
                        "Position.Longitude",
                        "Position.Speed"
                    }
                }
            },
            Timestamps =
            {
                timestamp
            },
            Rows =
            {
                new SampleRow
                {
                    DoubleSamples = new DoubleSampleList
                    {
                        Samples =
                        {
                            new DoubleSample
                            {
                                Value = GenerateLatitude(),
                                Status = DataStatus.Valid
                            },
                            new DoubleSample
                            {
                                Value = GenerateLongitude(),
                                Status = DataStatus.Valid
                            },
                            new DoubleSample
                            {
                                Value = GenerateSpeed(),
                                Status = DataStatus.Valid
                            }
                        }
                    }
                }
            }
        };

        await this.packetWriter.WriteDataPacketAsync(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = "position",
                    SessionKey = this.SessionKey,
                    Message = new Packet
                    {
                        Content = positionData.ToByteString(),
                        Type = "RowData",
                        IsEssential = false,
                        SessionKey = this.SessionKey
                    }
                }
            });
    }

    private async Task CheckForEvents(ulong timestamp)
    {
        var engineTemp = GenerateEngineTemp();

        // Check for overtemperature event
        if (engineTemp > 120.0)
        {
            var eventPacket = new EventPacket
            {
                DataFormat = new EventDataFormat
                {
                    EventIdentifier = "Engine.OverTemp"
                },
                Timestamp = timestamp,
                RawValues =
                {
                    engineTemp
                }
            };

            await this.packetWriter.WriteDataPacketAsync(
                new WriteDataPacketRequest
                {
                    Detail = new DataPacketDetails
                    {
                        DataSource = this.DataSource,
                        Stream = "events",
                        SessionKey = this.SessionKey,
                        Message = new Packet
                        {
                            Content = eventPacket.ToByteString(),
                            Type = "Event",
                            IsEssential = false,
                            SessionKey = this.SessionKey
                        }
                    }
                });
        }

    }

    // Simulation methods
    private static double GenerateRpm()
    {
        return 2000 + Random.Shared.NextDouble() * 4000;
    }

    private static double GenerateEngineTemp()
    {
        return 85 + Random.Shared.NextDouble() * 50;
    }

    private static double GenerateOilPressure()
    {
        return 4.5 + Random.Shared.NextDouble() * 2.0;
    }

    private static double GenerateLatitude()
    {
        return 52.0747 + Random.Shared.NextDouble() * 0.001;
    }

    private static double GenerateLongitude()
    {
        return -1.0174 + Random.Shared.NextDouble() * 0.001;
    }

    private static double GenerateSpeed()
    {
        return Random.Shared.NextDouble() * 200;
    }
}
