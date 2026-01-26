// <copyright file="PartitionBasedPacketWriteAndBatchReadShould.cs" company="Motion Applied Ltd.">
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

using System.Diagnostics;
using System.Text.Json;

using FluentAssertions;

using Google.Protobuf;

using Grpc.Core;

using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.IntegrationTests.Base;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class PartitionBasedPacketWriteAndBatchReadShould
{
    private const string BrokerUrl = "localhost:9098";
    private const string AppGroup1Stream = "AppGroup1Stream";
    private const string AppGroup2Stream = "AppGroup2Stream";
    private const string DataSource = "PBPWBRDataSource";
    private const string Domain = "PBPWBRDomain";
    private const string SampleType = "SampleType";
    private const string StreamStartedType = "StreamStarted";
    private static PacketWriterService.PacketWriterServiceClient? packetWriter;
    private static AsyncServerStreamingCall<ReadPacketsResponse>? receiveStream;
    private static PacketReaderService.PacketReaderServiceClient? packetReader;
    private static NewConnectionResponse? receiveConnection;
    private static StreamingApiClient? streamingApiClient;
    private static bool initialised;
    private readonly ITestOutputHelper outputHelper;
    private const string SessionKeyPrefix = "PBPWBRSessionKey";
    private static string sessionKey = "";
    public PartitionBasedPacketWriteAndBatchReadShould(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
        if (initialised)
        {
            CreateNewResponseStreamForNewSessionKey();

            return;
        }

        var streamConfiguration = new StreamingApiConfiguration(
            StreamCreationStrategy.PartitionBased,
            BrokerUrl,
            [
                new PartitionMapping(AppGroup1Stream, 1),
                new PartitionMapping(AppGroup2Stream, 2)
            ],
            integrateDataFormatManagement: false,
            integrateSessionManagement: false,
            batchingResponses: true,
            domain: Domain,
            streamApiPort: ApiPortCreator.CreateNew());

        var cancellationTokenSourceProvider = new CancellationTokenSourceProvider();

        streamingApiClient = StreamingApiClientFactory.Create(
            streamConfiguration,
            cancellationTokenSourceProvider,
            new KafkaBrokerAvailabilityChecker(),
            new LoggingDirectoryProvider(""));

        outputHelper.WriteLine($"Initialising Streaming API Client=>{DataSource}...{Environment.NewLine}{JsonSerializer.Serialize(streamConfiguration)}");
        streamingApiClient.Initialise();

        Task.Delay(2000).Wait();

        packetWriter = streamingApiClient.GetPacketWriterClient();
        packetReader = streamingApiClient.GetPacketReaderClient();
        CreateNewResponseStreamForNewSessionKey();
        initialised = true;
    }

    private static void CreateNewResponseStreamForNewSessionKey()
    {
        if (streamingApiClient is null || packetReader is null)
        {
            throw new Exception("the stream api client didn't initialised properly");
        }

        sessionKey = SessionCreator.Create(SessionKeyPrefix);
        receiveConnection = streamingApiClient.GetConnectionManagerClient().NewConnection(
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
                    },
                    ExcludeMainStream = false
                }
            });
        receiveStream = packetReader.ReadPackets(
            new ReadPacketsRequest
            {
                Connection = new Connection
                {
                    Id = receiveConnection.Connection.Id
                }
            });
    }

    [Fact]
    public async Task Publish_One_DataPacket_Using_AppGroup1Stream_And_SessionKey_To_Partition_One()
    {
        // Arrange
        var content = ByteString.CopyFrom([1, 2, 3]);

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
                    SessionKey = sessionKey,
                    Type = SampleType
                },
                SessionKey = sessionKey
            }
        };
        if (receiveStream is null)
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
                while (await receiveStream.ResponseStream.MoveNext(token))
                {
                    var currentResponse = receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(packetResponse => packetResponse.Stream == AppGroup1Stream &&
                                                               packetResponse.Packet.Type == SampleType))
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
        await tokenSource.CancelAsync();
        //assert
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_DataPacket_Using_AppGroup2Stream_And_SessionKey_To_Partition_Two()
    {
        // Arrange

        var content = ByteString.CopyFrom([4, 5, 6]);
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
                    SessionKey = sessionKey,
                    Type = SampleType
                },
                SessionKey = sessionKey
            }
        };
        if (receiveStream is null)
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
                while (await receiveStream.ResponseStream.MoveNext(token))
                {
                    var currentResponse = receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(packetResponse => packetResponse.Stream == AppGroup2Stream &&
                                                               packetResponse.Packet.Type == SampleType))
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
        await tokenSource.CancelAsync();
        //assert
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_DataPacket_Using_UnknownStream_And_SessionKey_To_Partition_Zero()
    {
        // Arrange
        var content = ByteString.CopyFrom([7, 8, 9]);
        var writeDataPacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                Stream = "Unknown",
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = sessionKey,
                    Type = SampleType
                },
                SessionKey = sessionKey
            }
        };
        if (receiveStream is null)
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
                while (await receiveStream.ResponseStream.MoveNext(token))
                {
                    var currentResponse = receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(packetResponse => string.IsNullOrEmpty(packetResponse.Stream) &&
                                                               packetResponse.Packet.Type == SampleType))
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
        await tokenSource.CancelAsync();

        //assert
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_DataPacket_With_SessionKey_Without_Stream_To_Partition_Zero()
    {
        // Arrange
        var content = ByteString.CopyFrom([10, 11, 12]);
        var writeDataPacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = sessionKey,
                    Type = SampleType
                },
                SessionKey = sessionKey
            }
        };
        if (receiveStream is null)
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
                while (await receiveStream.ResponseStream.MoveNext(token))
                {
                    var currentResponse = receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(packetResponse => string.IsNullOrEmpty(packetResponse.Stream) &&
                                                               packetResponse.Packet.Type == SampleType))
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
        await tokenSource.CancelAsync();
        //assert
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_DataPackets_With_SessionKey_To_Specified_Partition_Based_on_Stream()
    {
        // Arrange
        var content1 = ByteString.CopyFrom([13, 14, 15]);
        var content2 = ByteString.CopyFrom([16, 17, 18]);
        var content3 = ByteString.CopyFrom([19, 20, 21]);

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
                    SessionKey = sessionKey,
                    Type = SampleType
                },
                SessionKey = sessionKey
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
                    SessionKey = sessionKey,
                    Type = SampleType
                },
                SessionKey = sessionKey
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
                    SessionKey = sessionKey,
                    Type = SampleType
                },
                SessionKey = sessionKey
            }
        };
        if (receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent1 = new AutoResetEvent(false);
        var autoResetEvent2 = new AutoResetEvent(false);
        var autoResetEvent3 = new AutoResetEvent(false);
        var autoResetEvent4 = new AutoResetEvent(false);
        var autoResetEvent5 = new AutoResetEvent(false);
        var autoResetEvent6 = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        List<bool> assertResult = [false, false, false, false, false, false];
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await receiveStream.ResponseStream.MoveNext(token))
                {
                    var currentResponse = receiveStream.ResponseStream.Current.Response;
                    foreach (var packetResponse in currentResponse)
                    {
                        switch (packetResponse.Stream)
                        {
                            case AppGroup1Stream:
                                if (packetResponse.Packet.Type == StreamStartedType)
                                {
                                    assertResult[3] = true;
                                    autoResetEvent4.Set();
                                }

                                if (packetResponse.Packet.Content.SequenceEqual(content1))
                                {
                                    assertResult[0] = true;
                                    autoResetEvent1.Set();
                                }

                                break;
                            case AppGroup2Stream:
                                if (packetResponse.Packet.Type == StreamStartedType)
                                {
                                    assertResult[4] = true;
                                    autoResetEvent5.Set();
                                }

                                if (packetResponse.Packet.Content.SequenceEqual(content2))
                                {
                                    assertResult[1] = true;
                                    autoResetEvent2.Set();
                                }

                                break;
                            case "":
                                if (packetResponse.Packet.Type == StreamStartedType)
                                {
                                    assertResult[5] = true;
                                    autoResetEvent6.Set();
                                }

                                if (packetResponse.Packet.Content.SequenceEqual(content3))
                                {
                                    assertResult[2] = true;
                                    autoResetEvent3.Set();
                                }

                                break;
                        }
                    }

                    if (assertResult.All(i => i))
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
        autoResetEvent4.WaitOne(TimeSpan.FromSeconds(10));
        autoResetEvent5.WaitOne(TimeSpan.FromSeconds(10));
        autoResetEvent6.WaitOne(TimeSpan.FromSeconds(10));
        await tokenSource.CancelAsync();
        //assert
        assertResult.ForEach(i => i.Should().BeTrue());
    }

    [Fact]
    public async Task When_Publish_Data_With_Different_SessionKey_Than_The_Receive_SessionKey_The_Data_Should_Net_Be_Delivered()
    {
        // Arrange
        var content = ByteString.CopyFrom([22, 23, 24]);
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
                    Type = SampleType
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
                    SessionKey = sessionKey,
                    Type = SampleType
                },
                SessionKey = sessionKey
            }
        };
        if (receiveStream is null)
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
                if (await receiveStream.ResponseStream.MoveNext())
                {
                    if (receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.SessionKey == UnknownSessionKey))
                    {
                        receiveCount++;
                    }

                    if (receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.SessionKey == sessionKey))
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
        await tokenSource.CancelAsync();
        //assert
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task When_Publish_1000_Data_Using_WriteStream_And_SessionKey_All_Messages_Should_Be_Delivered()
    {
        // Arrange
        if (receiveStream is null ||
            packetWriter is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        var writeStream = packetWriter.WriteDataPackets();
        var rnd = new Random();
        const int NumberOfMessages = 1000;
        // The 3 is for the new session packets 
        var numberOfExpectedMessages = NumberOfMessages + 3;
        var lstContents = new List<ByteString>(NumberOfMessages);
        var lstRequests = new List<WriteDataPacketsRequest>(NumberOfMessages);
        for (var i = 0; i < NumberOfMessages; i++)
        {
            var byteArr = new byte[3];
            rnd.NextBytes(byteArr);
            lstContents.Add(ByteString.CopyFrom(byteArr));
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
                                SessionKey = sessionKey,
                                Type = i.ToString()
                            },
                            SessionKey = sessionKey
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
                while (await receiveStream.ResponseStream.MoveNext(token))
                {
                    foreach (var _ in receiveStream.ResponseStream.Current.Response)
                    {
                        if (receiveCount == 0)
                        {
                            receiverStopWatch.Start();
                        }

                        receiveCount++;
                    }

                    if (receiveCount != numberOfExpectedMessages)
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
        await tokenSource.CancelAsync();
        //assert
        receiveCount.Should().Be(numberOfExpectedMessages);
    }

    [Fact]
    public async Task Publish_One_Essential_DataPacket_Read_All_Essentials_Should_Deliver_It_And_Packet_Should_Be_Received_On_Defined_DataSource_Stream_Too()
    {
        // Arrange
        if (receiveStream is null ||
            packetReader is null ||
            receiveConnection is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        var content = ByteString.CopyFrom([1, 2, 3]);
        var writeDataPacketRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                Stream = AppGroup1Stream,
                DataSource = DataSource,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = true,
                    SessionKey = sessionKey,
                    Type = SampleType
                },
                SessionKey = sessionKey
            }
        };
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await receiveStream.ResponseStream.MoveNext(token))
                {
                    var currentResponse = receiveStream.ResponseStream.Current.Response;
                    if (!currentResponse.Any(packetResponse => packetResponse.Stream == AppGroup1Stream &&
                                                               packetResponse.Packet.Type == SampleType &&
                                                               packetResponse.Packet.IsEssential))
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
        await this.WriteDataPacket(writeDataPacketRequest);
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(1));
        var lstResult = new List<ReadEssentialsResponse>();

        // Act
        var result = packetReader.ReadEssentials(
            new ReadEssentialsRequest
            {
                Connection = receiveConnection.Connection
            });
        var essentialAutoResetEvent = new AutoResetEvent(false);
        var counter = 0;
        _ = Task.Run(
            async () =>
            {
                while (await result.ResponseStream.MoveNext())
                {
                    lstResult.Add(result.ResponseStream.Current);
                    counter += result.ResponseStream.Current.Response.Count(j => j.Packet.SessionKey == sessionKey);
                    if (counter == 2)
                    {
                        essentialAutoResetEvent.Set();
                    }
                }
            },
            token);
        essentialAutoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        await tokenSource.CancelAsync();

        //Assert
        assertResult.Should().BeTrue();
        lstResult.Sum(i => i.Response.Count(j => j.Packet.SessionKey == sessionKey)).Should().Be(2);
    }

    [Fact]
    public async Task Publish_1000_Essential_DataPacket_Read_All_Essentials_Should_Deliver_All_And_Packet_Should_Be_Received_On_Defined_DataSource_Stream_Too()
    {
        // Arrange
        if (receiveStream is null ||
            packetReader is null ||
            receiveConnection is null ||
            packetWriter is null ||
            streamingApiClient is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        var writeStream = packetWriter.WriteDataPackets();
        var rnd = new Random();
        const int NumberOfMessages = 1000;
        var numberOfExpectedMessages = NumberOfMessages + 3;
        var lstContents = new List<ByteString>(NumberOfMessages);
        var lstRequests = new List<WriteDataPacketsRequest>(NumberOfMessages);
        for (var i = 0; i < NumberOfMessages; i++)
        {
            var byteArr = new byte[3];
            rnd.NextBytes(byteArr);
            lstContents.Add(ByteString.CopyFrom(byteArr));
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
                                IsEssential = true,
                                SessionKey = sessionKey,
                                Type = i.ToString()
                            },
                            SessionKey = sessionKey
                        }
                    }
                });
        }

        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var autoResetEvent = new AutoResetEvent(false);
        var receiveCount = 0;
        var receiverStopWatch = new Stopwatch();
        var startListenerAutoResetEvent = new AutoResetEvent(false);
        _ = Task.Run(
            async () =>
            {
                startListenerAutoResetEvent.Set();
                while (await receiveStream.ResponseStream.MoveNext(token))
                {
                    foreach (var _ in receiveStream.ResponseStream.Current.Response.Where(i => i.Packet.SessionKey == sessionKey))
                    {
                        if (receiveCount == 0)
                        {
                            receiverStopWatch.Start();
                        }

                        receiveCount++;
                    }

                    if (receiveCount != numberOfExpectedMessages)
                    {
                        continue;
                    }

                    this.outputHelper.WriteLine($"receivingTime : {receiverStopWatch.ElapsedMilliseconds} ms");
                    autoResetEvent.Set();
                    break;
                }
            },
            token);
        startListenerAutoResetEvent.WaitOne(5000);
        // Act
        var publishStopWatch = new Stopwatch();
        publishStopWatch.Start();
        foreach (var writeDataPacketsRequest in lstRequests)
        {
            await writeStream.RequestStream.WriteAsync(writeDataPacketsRequest, token);
        }

        publishStopWatch.Stop();
        this.outputHelper.WriteLine($"publishing time: {publishStopWatch.ElapsedMilliseconds} ms");
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(20));

        var lstResult = new List<ReadEssentialsResponse>();
        var connectionResponse = await streamingApiClient.GetConnectionManagerClient().NewConnectionAsync(
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
                    },
                    ExcludeMainStream = false
                }
            });
        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(5));
        var result = packetReader.ReadEssentials(
            new ReadEssentialsRequest
            {
                Connection = connectionResponse.Connection
            });
        var essentialAutoResetEvent = new AutoResetEvent(false);
        var counter = 0;
        _ = Task.Run(
            async () =>
            {
                while (await result.ResponseStream.MoveNext())
                {
                    lstResult.Add(result.ResponseStream.Current);
                    counter += result.ResponseStream.Current.Response.Count(j => j.Packet.SessionKey == sessionKey);
                    if (counter == NumberOfMessages + 1)
                    {
                        essentialAutoResetEvent.Set();
                    }
                }
            },
            token);
        essentialAutoResetEvent.WaitOne(TimeSpan.FromSeconds(10));
        await tokenSource.CancelAsync();
        //assert
        receiveCount.Should().Be(numberOfExpectedMessages);
        lstResult.Sum(i => i.Response.Count(j => j.Packet.SessionKey == sessionKey)).Should().Be(1001);
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
}
