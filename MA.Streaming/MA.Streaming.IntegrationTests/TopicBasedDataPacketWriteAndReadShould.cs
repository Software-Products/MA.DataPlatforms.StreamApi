// <copyright file="TopicBasedDataPacketWriteAndReadShould.cs" company="McLaren Applied Ltd.">
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

using System.Diagnostics;
using FluentAssertions;

using Google.Protobuf;
using Grpc.Core;

using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.IntegrationTests.Base;
using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class TopicBasedDataPacketWriteAndReadShould : IClassFixture<KafkaTestsCleanUpFixture>
{
    private const string BrokerUrl = "localhost:9097";
    private const string AppGroup1Stream = "AppGroup1Stream";
    private const string AppGroup2Stream = "AppGroup2Stream";
    private const string DataSource = "TopicBased_Data_Not_Batched_SampleDataSource";
    private const string SampleType = "SampleType";
    private const string PeriodicDataType = "PeriodicData";
    private const string RowDataType = "RowData";
    private const string SynchroDataType = "SynchroData";
    private const string EventType = "Event";
    private const string MarkerType = "Marker";

    private static PacketWriterService.PacketWriterServiceClient? packetWriter;
    private static PacketReaderService.PacketReaderServiceClient? packetReader;
    private static DataFormatManagerService.DataFormatManagerServiceClient? dataFormatManager;
    private static ConnectionManagerService.ConnectionManagerServiceClient? connectionManager;
    private static bool initialised;

    private AsyncServerStreamingCall<ReadDataPacketsResponse>? receiveStream;
    private NewConnectionResponse? receiveConnection;
    private CreateDataPacketHelper? createDataPacketHelper;
    private readonly ITestOutputHelper outputHelper;

    public TopicBasedDataPacketWriteAndReadShould(ITestOutputHelper outputHelper, KafkaTestsCleanUpFixture _)
    {
        this.outputHelper = outputHelper;
        if (initialised)
        {
            if (dataFormatManager is null)
            {
                throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
            }
            this.createDataPacketHelper = new CreateDataPacketHelper(DataSource, dataFormatManager, this.outputHelper);
            return;
        }

        var streamConfiguration = new StreamingApiConfiguration(
            StreamCreationStrategy.TopicBased,
            BrokerUrl,
            [
                new PartitionMapping(AppGroup1Stream, 1),
                new PartitionMapping(AppGroup2Stream, 2)
            ],
            integrateDataFormatManagement: true,
            integrateSessionManagement: false,
            batchingResponses: false);
        var cancellationTokenSourceProvider = new CancellationTokenSourceProvider();
        StreamingApiClient.Initialise(streamConfiguration, cancellationTokenSourceProvider, new KafkaBrokerAvailabilityChecker(), new LoggingDirectoryProvider(""));
        packetWriter = StreamingApiClient.GetPacketWriterClient();
        packetReader = StreamingApiClient.GetPacketReaderClient();
        dataFormatManager = StreamingApiClient.GetDataFormatManagerClient();
        connectionManager = StreamingApiClient.GetConnectionManagerClient();

        initialised = true;
        this.createDataPacketHelper = new CreateDataPacketHelper(DataSource, dataFormatManager, this.outputHelper);
    }

    // parity with read packets
    [Fact]
    public async Task Publish_One_PeriodicDataPacket_Using_AppGroup1Stream_And_SessionKey_To_AppStream1_Topic()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var runSessionId = Guid.NewGuid().ToString();
        ConfigureReceiveStream(runSessionId);

        var parameterIdentifiers = new List<string> { "IncludeParameter" };
        PeriodicDataPacket periodicDataPacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(parameterIdentifiers);

        var content = periodicDataPacket.ToByteString();
        var packetType = PeriodicDataType;
        var writeDataPacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                Stream = AppGroup1Stream,
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    this.outputHelper.WriteLine("Received Packet");
                    var currentResponse = this.receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(
                            packetResponse => packetResponse.Stream == AppGroup1Stream &&
                                              packetResponse.Packet.SessionKey == runSessionId &&
                                              packetResponse.Packet.Type == packetType &&
                                              packetResponse.Packet.Content.SequenceEqual(content)
                        ))
                    {
                        continue;
                    }

                    assertResult = true;
                    autoResetEvent.Set();
                    break;
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeDataPacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_SynchroDataPacket_Using_AppGroup2Stream_And_SessionKey_To_AppStream2_Topic()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var parameters = new List<string>() { "IncludeParameter" };
        var runSessionId = Guid.NewGuid().ToString();

        ConfigureReceiveStream(runSessionId);
        SynchroDataPacket synchroDataPacket = await this.createDataPacketHelper.CreateSynchroDataPacket(parameters);

        var content = synchroDataPacket.ToByteString();
        var packetType = SynchroDataType;
        var writeDataPacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                Stream = AppGroup2Stream,
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    var currentResponse = this.receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(
                            packetResponse => packetResponse.Stream == AppGroup2Stream &&
                                              packetResponse.Packet.SessionKey == runSessionId &&
                                              packetResponse.Packet.Type == packetType &&
                                              packetResponse.Packet.Content.SequenceEqual(content)))
                    {
                        continue;
                    }

                    assertResult = true;
                    autoResetEvent.Set();
                    break;
                }
            },
            token);
        startListenerAutoResetEvent.Set();
        // Act
        await this.WriteDataPacket(writeDataPacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        assertResult.Should().BeTrue();
    }
    [Fact]
    public async Task Publish_One_RowDataPacket_Using_AppGroup2Stream_And_SessionKey_To_AppStream2_Topic()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var parameters = new List<string>() { "IncludeParameter" };
        var runSessionId = Guid.NewGuid().ToString();

        ConfigureReceiveStream(runSessionId);
        RowDataPacket rowDataPacket = await this.createDataPacketHelper.CreateRowDataPacket(parameters);

        var content = rowDataPacket.ToByteString();
        var packetType = RowDataType;
        var writeDataPacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                Stream = AppGroup2Stream,
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    var currentResponse = this.receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(
                            packetResponse => packetResponse.Stream == AppGroup2Stream &&
                                              packetResponse.Packet.SessionKey == runSessionId &&
                                              packetResponse.Packet.Type == packetType &&
                                              packetResponse.Packet.Content.SequenceEqual(content)))
                    {
                        continue;
                    }

                    assertResult = true;
                    autoResetEvent.Set();
                    break;
                }
            },
            token);
        startListenerAutoResetEvent.Set();
        // Act
        await this.WriteDataPacket(writeDataPacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_DataPackets_With_SessionKey_To_Specified_Topic_Based_on_Stream()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var parameters = new List<string>() { "IncludeParameter" };
        var runSessionId = Guid.NewGuid().ToString();

        ConfigureReceiveStream(runSessionId);
        var rowDataPacket1 = await this.createDataPacketHelper.CreateRowDataPacket(parameters, 1001);
        var rowDataPacket2 = await this.createDataPacketHelper.CreateRowDataPacket(parameters, 1002);
        var periodicDataPacket1 = await this.createDataPacketHelper.CreatePeriodicDataPacket(parameters, 1003);

        var content1 = rowDataPacket1.ToByteString();
        var content2 = rowDataPacket2.ToByteString();
        var content3 = periodicDataPacket1.ToByteString();
        var writeDataPacketRequest1 = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = content1,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = RowDataType
                },
                SessionKey = runSessionId
            }
        };
        var writeDataPacketRequest2 = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup2Stream,
                Message = new Packet
                {
                    Content = content2,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = RowDataType
                },
                SessionKey = runSessionId
            }
        };
        var writeDataPacketRequest3 = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = content3,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = PeriodicDataType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent1 = new AutoResetEvent(false);
        var autoResetEvent2 = new AutoResetEvent(false);
        var autoResetEvent3 = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        List<bool> assertResult = [false, false, false];
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    var currentResponse = this.receiveStream.ResponseStream.Current.Response;
                    foreach (var packetResponse in currentResponse.Where(packetResponse => packetResponse.Packet.SessionKey == runSessionId))
                    {
                        this.outputHelper.WriteLine("packet received.");
                        switch (packetResponse.Stream)
                        {
                            case AppGroup1Stream:
                                if (packetResponse.Packet.Content.SequenceEqual(content1))
                                {
                                    assertResult[0] = true;
                                    autoResetEvent1.Set();
                                }

                                break;
                            case AppGroup2Stream:

                                if (packetResponse.Packet.Content.SequenceEqual(content2))
                                {
                                    assertResult[1] = true;
                                    autoResetEvent2.Set();
                                }

                                break;
                            case "":
                                if (packetResponse.Packet.Content.SequenceEqual(content3))
                                {
                                    assertResult[2] = true;
                                    autoResetEvent3.Set();
                                }

                                break;
                        }
                    }

                    if (assertResult[0] &&
                        assertResult[1] &&
                        assertResult[2])
                    {
                        break;
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeDataPacketRequest1);
        await this.WriteDataPacket(writeDataPacketRequest2);
        await this.WriteDataPacket(writeDataPacketRequest3);
        autoResetEvent1.WaitOne(TimeSpan.FromSeconds(10));
        autoResetEvent2.WaitOne(TimeSpan.FromSeconds(10));
        autoResetEvent3.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        assertResult.ForEach(i => i.Should().BeTrue());
    }

    [Fact]
    public async Task When_Publish_Data_With_Different_SessionKey_Than_The_Receive_SessionKey_The_Data_Should_Net_Be_Delivered()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var parameters = new List<string>() { "IncludeParameter" };
        var runSessionId = Guid.NewGuid().ToString();

        ConfigureReceiveStream(runSessionId);
        var periodicDataPacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(parameters, 4001);

        var content = periodicDataPacket.ToByteString();
        const string UnknownSessionKey = "unknown";
        var writeDataPacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = UnknownSessionKey,
                    Type = PeriodicDataType
                },
                SessionKey = UnknownSessionKey
            }
        };
        var writeDataPacketRequest2 = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = PeriodicDataType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.SessionKey == UnknownSessionKey))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.SessionKey == runSessionId))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeDataPacketRequest);
        await this.WriteDataPacket(writeDataPacketRequest2);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task When_Publish_1000_Data_Using_WriteStream_And_SessionKey_All_Messages_Should_Be_Delivered()
    {
        // Arrange
        if (packetWriter is null ||
        this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        var parameters = new List<string>() { "IncludeParameter" };
        var runSessionId = Guid.NewGuid().ToString();

        ConfigureReceiveStream(runSessionId);
        var writeStream = packetWriter.WriteDataPackets();
        var rnd = new Random();
        const int NumberOfMessages = 1000;
        var lstContents = new List<ByteString>(NumberOfMessages);
        var lstRequests = new List<WriteDataPacketsRequest>(NumberOfMessages);
        for (var i = 0; i < NumberOfMessages; i++)
        {
            var periodicDataPacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(parameters, rnd.Next());
            var content = periodicDataPacket.ToByteString();
            lstContents.Add(content);
            var stream = i % 3 == 1 ? AppGroup1Stream : AppGroup2Stream;
            lstRequests.Add(
                new WriteDataPacketsRequest
                {
                    Details =
                    {
                    new DataPacketDetails
                    {
                        Stream = i % 3 == 0 ? "" : stream,
                        DataSource = DataSource,
                        Message = new Packet
                        {
                            Content = lstContents[i],
                            IsEssential = false,
                            SessionKey = runSessionId,
                            Type = PeriodicDataType
                        },
                        SessionKey = runSessionId
                    }
                    }
                });
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        var receiverStopWatch = new Stopwatch();
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    foreach (var _ in this.receiveStream.ResponseStream.Current.Response.Where(i => i.Packet.SessionKey == runSessionId))
                    {
                        if (receiveCount == 0)
                        {
                            receiverStopWatch.Start();
                        }

                        receiveCount++;
                    }

                    if (receiveCount != NumberOfMessages)
                    {
                        continue;
                    }

                    this.outputHelper.WriteLine($"receivingTime : {receiverStopWatch.ElapsedMilliseconds} ms");
                    autoResetEvent.Set();
                    break;
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        var publishStopWatch = new Stopwatch();
        publishStopWatch.Start();
        foreach (var writeDataPacketsRequest in lstRequests)
        {
            await writeStream.RequestStream.WriteAsync(writeDataPacketsRequest, token);
        }

        publishStopWatch.Stop();
        this.outputHelper.WriteLine($"publishing time: {publishStopWatch.ElapsedMilliseconds} ms");
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(1000);
    }

    // Read data packet specific
    [Fact]
    public async Task Publish_One_EventPacket_Using_AppGroup1Stream_And_SessionKey_To_AppStream1_Topic()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        string eventName = "IncludeEvent";
        var runSessionId = Guid.NewGuid().ToString();

        ConfigureReceiveStream(runSessionId);
        EventPacket eventPacket = await this.createDataPacketHelper.CreateEventPacket(eventName);

        var packetType = EventType;
        var content = eventPacket.ToByteString();
        var writeDataPacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                Stream = AppGroup1Stream,
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    this.outputHelper.WriteLine("Received Packet");
                    var currentResponse = this.receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(
                            packetResponse => packetResponse.Stream == AppGroup1Stream &&
                                              packetResponse.Packet.SessionKey == runSessionId &&
                                              packetResponse.Packet.Type == packetType &&
                                              packetResponse.Packet.Content.SequenceEqual(content)
                        ))
                    {
                        continue;
                    }

                    assertResult = true;
                    autoResetEvent.Set();
                    break;
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeDataPacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_MarkerPacket_Using_AppGroup1Stream_And_SessionKey_To_AppStream1_Topic()
    {
        // Arrange
        var runSessionId = Guid.NewGuid().ToString();

        ConfigureReceiveStream(runSessionId, new DataPacketRequest
        {
            IncludeMarkers = true,
        });
        MarkerPacket markerPacket = new MarkerPacket
        {
            Timestamp = 1732898780000123456,
            Label = "Test Marker",
            Type = "Lap",
            Value = 1
        };

        var packetType = MarkerType;
        var content = markerPacket.ToByteString();
        var writeDataPacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                Stream = AppGroup1Stream,
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    this.outputHelper.WriteLine("Received Packet");
                    var currentResponse = this.receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(
                            packetResponse => packetResponse.Stream == AppGroup1Stream &&
                                              packetResponse.Packet.SessionKey == runSessionId &&
                                              packetResponse.Packet.Type == packetType &&
                                              packetResponse.Packet.Content.SequenceEqual(content)
                        ))
                    {
                        continue;
                    }

                    assertResult = true;
                    autoResetEvent.Set();
                    break;
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeDataPacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_MarkerPacket_And_Not_Be_Delivered_When_IncludeMarkers_Is_Set_To_False()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var parameters = new List<string>() { "IncludeParameter" };
        var runSessionId = Guid.NewGuid().ToString();

        ConfigureReceiveStream(runSessionId, new DataPacketRequest
        {
            IncludeMarkers = false,
        });
        MarkerPacket markerPacket = new MarkerPacket
        {
            Timestamp = 1732898780000123456,
            Label = "Test Marker",
            Type = "Lap",
            Value = 1
        };
        var periodicDataPacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(parameters, 4001);

        var periodicContent = periodicDataPacket.ToByteString();
        var markerConent = markerPacket.ToByteString();
        var writeDataPacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = markerConent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = MarkerType
                },
                SessionKey = runSessionId
            }
        };
        var writeDataPacketRequest2 = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = periodicContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = PeriodicDataType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Type == MarkerType))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.SessionKey == runSessionId))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeDataPacketRequest);
        await this.WriteDataPacket(writeDataPacketRequest2);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Only_Deliver_Periodic_Packets_Filtered_By_IncludeParameters()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }

        var includeParameters = new List<string>() { "IncludeParameter123" };
        var excludeParameters = new List<string>() { "ExcludeParameter123" };
        var runSessionId = Guid.NewGuid().ToString();
        var dataPacketRequest = new DataPacketRequest();
        dataPacketRequest.IncludeParameters.Add(includeParameters);

        ConfigureReceiveStream(runSessionId, dataPacketRequest);
        var includePacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(includeParameters, 4001);
        var excludePacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(excludeParameters, 4002);

        var packetType = PeriodicDataType;
        var includeContent = includePacket.ToByteString();
        var excludeContent = excludePacket.ToByteString();
        var writeIncludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = includeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        var writeExcludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = excludeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(excludeContent)))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(includeContent)))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeExcludePacketRequest);
        await this.WriteDataPacket(writeIncludePacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Only_Deliver_Row_Packets_Filtered_By_IncludeParameters()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var includeParameters = new List<string>() { "IncludeParameter123" };
        var excludeParameters = new List<string>() { "ExcludeParameter123" };
        var runSessionId = Guid.NewGuid().ToString();
        var dataPacketRequest = new DataPacketRequest();
        dataPacketRequest.IncludeParameters.Add(includeParameters);

        ConfigureReceiveStream(runSessionId, dataPacketRequest);
        var includePacket = await this.createDataPacketHelper.CreateRowDataPacket(includeParameters, 4001);
        var excludePacket = await this.createDataPacketHelper.CreateRowDataPacket(excludeParameters, 4002);

        var packetType = RowDataType;
        var includeContent = includePacket.ToByteString();
        var excludeContent = excludePacket.ToByteString();

        var writeIncludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = includeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        var writeExcludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = excludeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(excludeContent)))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(includeContent)))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeExcludePacketRequest);
        await this.WriteDataPacket(writeIncludePacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Only_Deliver_Synchro_Packets_Filtered_By_IncludeParameters()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var includeParameters = new List<string>() { "IncludeParameter123" };
        var excludeParameters = new List<string>() { "ExcludeParameter123" };
        var runSessionId = Guid.NewGuid().ToString();
        var dataPacketRequest = new DataPacketRequest();
        dataPacketRequest.IncludeParameters.Add(includeParameters);

        ConfigureReceiveStream(runSessionId, dataPacketRequest);
        var includePacket = await this.createDataPacketHelper.CreateSynchroDataPacket(includeParameters, 4001);
        var excludePacket = await this.createDataPacketHelper.CreateSynchroDataPacket(excludeParameters, 4002);

        var packetType = SynchroDataType;
        var includeContent = includePacket.ToByteString();
        var excludeContent = excludePacket.ToByteString();

        var writeIncludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = includeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        var writeExcludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = excludeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(excludeContent)))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(includeContent)))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeExcludePacketRequest);
        await this.WriteDataPacket(writeIncludePacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Only_Deliver_Periodic_Packets_Not_Filtered_By_ExcludeParameters()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var includeParameters = new List<string>() { "IncludeParameter123" };
        var excludeParameters = new List<string>() { "ExcludeParameter123" };
        var runSessionId = Guid.NewGuid().ToString();
        var dataPacketRequest = new DataPacketRequest();
        dataPacketRequest.ExcludeParameters.Add(excludeParameters);

        ConfigureReceiveStream(runSessionId, dataPacketRequest);
        var includePacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(includeParameters, 4001);
        var excludePacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(excludeParameters, 4002);

        var packetType = PeriodicDataType;
        var includeContent = includePacket.ToByteString();
        var excludeContent = excludePacket.ToByteString();

        var writeIncludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = includeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        var writeExcludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = excludeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(excludeContent)))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(includeContent)))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeExcludePacketRequest);
        await this.WriteDataPacket(writeIncludePacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Only_Deliver_Row_Packets_Not_Filtered_By_ExcludeParameters()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var includeParameters = new List<string>() { "IncludeParameter123" };
        var excludeParameters = new List<string>() { "ExcludeParameter123" };
        var runSessionId = Guid.NewGuid().ToString();
        var dataPacketRequest = new DataPacketRequest();
        dataPacketRequest.ExcludeParameters.Add(excludeParameters);

        ConfigureReceiveStream(runSessionId, dataPacketRequest);
        var includePacket = await this.createDataPacketHelper.CreateRowDataPacket(includeParameters, 4001);
        var excludePacket = await this.createDataPacketHelper.CreateRowDataPacket(excludeParameters, 4002);

        var packetType = RowDataType;
        var includeContent = includePacket.ToByteString();
        var excludeContent = excludePacket.ToByteString();

        var writeIncludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = includeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        var writeExcludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = excludeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(excludeContent)))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(includeContent)))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeExcludePacketRequest);
        await this.WriteDataPacket(writeIncludePacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Only_Deliver_Synchro_Packets_Not_Filtered_By_ExcludeParameters()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var includeParameters = new List<string>() { "IncludeParameter123" };
        var excludeParameters = new List<string>() { "ExcludeParameter123" };
        var runSessionId = Guid.NewGuid().ToString();
        var dataPacketRequest = new DataPacketRequest();
        dataPacketRequest.ExcludeParameters.Add(excludeParameters);

        ConfigureReceiveStream(runSessionId, dataPacketRequest);
        var includePacket = await this.createDataPacketHelper.CreateSynchroDataPacket(includeParameters, 4001);
        var excludePacket = await this.createDataPacketHelper.CreateSynchroDataPacket(excludeParameters, 4002);

        var packetType = SynchroDataType;
        var includeContent = includePacket.ToByteString();
        var excludeContent = excludePacket.ToByteString();

        var writeIncludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = includeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        var writeExcludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = excludeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(excludeContent)))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(includeContent)))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeExcludePacketRequest);
        await this.WriteDataPacket(writeIncludePacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Only_Deliver_Event_Packets_Filtered_By_IncludeEvents()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var includeEvents = "IncludeEvents321";
        var excludeEvents = "ExcludeEvents321";
        var runSessionId = Guid.NewGuid().ToString();
        var dataPacketRequest = new DataPacketRequest();
        dataPacketRequest.IncludeEvents.Add(includeEvents);

        ConfigureReceiveStream(runSessionId, dataPacketRequest);
        var includePacket = await this.createDataPacketHelper.CreateEventPacket(includeEvents);
        var excludePacket = await this.createDataPacketHelper.CreateEventPacket(excludeEvents);

        var packetType = EventType;
        var includeContent = includePacket.ToByteString();
        var excludeContent = excludePacket.ToByteString();

        var writeIncludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = includeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        var writeExcludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = excludeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(excludeContent)))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(includeContent)))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeExcludePacketRequest);
        await this.WriteDataPacket(writeIncludePacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Only_Deliver_Event_Packets_Not_Filtered_By_ExcludeEvents()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }
        var includeEvents = "IncludeEvents321";
        var excludeEvents = "ExcludeEvents321";
        var runSessionId = Guid.NewGuid().ToString();
        var dataPacketRequest = new DataPacketRequest();
        dataPacketRequest.ExcludeEvents.Add(excludeEvents);

        ConfigureReceiveStream(runSessionId, dataPacketRequest);
        var includePacket = await this.createDataPacketHelper.CreateEventPacket(includeEvents);
        var excludePacket = await this.createDataPacketHelper.CreateEventPacket(excludeEvents);

        var packetType = EventType;
        var includeContent = includePacket.ToByteString();
        var excludeContent = excludePacket.ToByteString();

        var writeIncludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = includeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        var writeExcludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = excludeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(excludeContent)))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(includeContent)))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeExcludePacketRequest);
        await this.WriteDataPacket(writeIncludePacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Prioritising_Exclude_List_And_Deliver_Periodic_Packets_Filtered()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }

        var includeParameters = new List<string> { "IncludeParameter123" };
        var excludeParameters = new List<string> { "ExcludeParameter123" };
        var runSessionId = Guid.NewGuid().ToString();
        var dataPacketRequest = new DataPacketRequest();
        dataPacketRequest.IncludeParameters.Add(includeParameters);
        dataPacketRequest.IncludeParameters.Add(excludeParameters);
        dataPacketRequest.ExcludeParameters.Add(excludeParameters);

        ConfigureReceiveStream(runSessionId, dataPacketRequest);
        var includePacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(includeParameters, 4001);
        var excludePacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(excludeParameters, 4002);

        var packetType = PeriodicDataType;
        var includeContent = includePacket.ToByteString();
        var excludeContent = excludePacket.ToByteString();
        var writeIncludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = includeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        var writeExcludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = excludeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(excludeContent)))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(includeContent)))
                    {
                        autoResetEvent.Set();
                    }
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeExcludePacketRequest);
        await this.WriteDataPacket(writeIncludePacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Still_Include_Periodic_Packet_If_Parameters_In_Both_Include_And_Exclude_List()
    {
        // Arrange
        if (this.createDataPacketHelper is null)
        {
            throw new InvalidOperationException("Create Data Packet Helper is null which shouldn't be");
        }

        var includeParameters = new List<string>() { "IncludeParameter123" };
        var excludeParameters = new List<string>() { "ExcludeParameter123" };
        var packetParameters = new List<string>();
        packetParameters.AddRange(includeParameters);
        packetParameters.AddRange(excludeParameters);

        var runSessionId = Guid.NewGuid().ToString();
        var dataPacketRequest = new DataPacketRequest();
        dataPacketRequest.IncludeParameters.Add(includeParameters);
        dataPacketRequest.ExcludeParameters.Add(excludeParameters);

        ConfigureReceiveStream(runSessionId, dataPacketRequest);
        var includePacket = await this.createDataPacketHelper.CreatePeriodicDataPacket(packetParameters, 4001);

        var packetType = PeriodicDataType;
        var includeContent = includePacket.ToByteString();
        var writeIncludePacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Stream = AppGroup1Stream,
                Message = new Packet
                {
                    Content = includeContent,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                },
                SessionKey = runSessionId
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    this.outputHelper.WriteLine("Received Packet");
                    var currentResponse = this.receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(
                            packetResponse => packetResponse.Stream == AppGroup1Stream &&
                                              packetResponse.Packet.SessionKey == runSessionId &&
                                              packetResponse.Packet.Type == packetType &&
                                              packetResponse.Packet.Content.SequenceEqual(includeContent)
                        ))
                    {
                        continue;
                    }

                    assertResult = true;
                    autoResetEvent.Set();
                    break;
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(TimeSpan.FromSeconds(5));
        // Act
        await this.WriteDataPacket(writeIncludePacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        tokenSource.Cancel();
        //assert
        assertResult.Should().BeTrue();
    }

    private async Task WriteDataPacket(WriteDataPacketRequest writeDataPacketRequest, bool log = true)
    {
        if (packetWriter is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        var publishStopWatch = new Stopwatch();
        publishStopWatch.Start();
        await packetWriter.WriteDataPacketAsync(writeDataPacketRequest);
        publishStopWatch.Stop();
        if (log)
        {
            this.outputHelper.WriteLine($"publishing time: {publishStopWatch.ElapsedMilliseconds} ms");
        }
    }

    private void ConfigureReceiveStream(string sessionKey, DataPacketRequest? request = null)
    {

        if (packetReader is null)
        {
            throw new InvalidOperationException("Packet reader is null which shouldn't be");
        }
        if (connectionManager is null)
        {
            throw new InvalidOperationException("Connection manager is null which shouldn't be");
        }
        if (request is null)
        {
            // If not specified, then allow everything. 
            request = new DataPacketRequest
            {
            };
        }
        this.receiveConnection = connectionManager.NewConnection(
            new NewConnectionRequest
            {
                Details = new ConnectionDetails
                {
                    DataSource = DataSource,
                    SessionKey = sessionKey,
                    StreamOffsets =
                    {
                                0,
                                0
                    },
                    Streams =
                    {
                                AppGroup1Stream,
                                AppGroup2Stream
                    }
                }
            });

        request.Connection = new Connection
        {
            Id = this.receiveConnection.Connection.Id
        };
        this.receiveStream = packetReader.ReadDataPackets(
            new ReadDataPacketsRequest
            {
                Request = request
            });
        this.outputHelper.WriteLine($"Connection ID: {this.receiveConnection.Connection.Id}");
    }

}
