// <copyright file="TopicBasedDataPacketWriteAndReadBase.cs" company="Motion Applied Ltd.">
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

using System.Collections.Concurrent;
using System.Diagnostics;

using FluentAssertions;

using Google.Protobuf;

using Grpc.Core;

using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.IntegrationTests.Helper;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests.Base;

public abstract class TopicBasedDataPacketWriteAndReadBase
{
    // -- per-concrete-type shared client state ---------------------------------

    private sealed class Clients
    {
        public PacketWriterService.PacketWriterServiceClient? Writer;
        public PacketReaderService.PacketReaderServiceClient? Reader;
        public DataFormatManagerService.DataFormatManagerServiceClient? DataFormatManager;
        public ConnectionManagerService.ConnectionManagerServiceClient? ConnectionManager;
    }

    private static readonly ConcurrentDictionary<Type, Clients> SharedClients = new();

    private Clients Shared => SharedClients.GetOrAdd(this.GetType(), _ => new Clients());

    // -- per-test-instance state -----------------------------------------------

    private AsyncServerStreamingCall<ReadDataPacketsResponse>? receiveStream;
    private NewConnectionResponse? receiveConnection;

    protected CreateDataPacketHelper? CreateDataPacketHelper;

    // -- configuration overrides -----------------------------------------------

    protected abstract string BrokerUrl { get; }

    protected abstract string DataSource { get; }

    protected abstract string Domain { get; }

    protected virtual string AppGroup1Stream => "AppGroup1Stream";

    protected virtual string AppGroup2Stream => "AppGroup2Stream";

    protected virtual string PeriodicDataType => "PeriodicData";

    protected virtual string RowDataType => "RowData";

    protected virtual string SynchroDataType => "SynchroData";

    protected virtual string EventType => "Event";

    protected virtual string MarkerType => "Marker";

    protected virtual IKafkaSecurityConfigurationProvider CreateSecurityProvider() =>
        new EmptyKafkaSecurityConfigProvider();

    private readonly ITestOutputHelper outputHelper;

    protected TopicBasedDataPacketWriteAndReadBase(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
        this.EnsureInitialised();
    }

    private void EnsureInitialised()
    {
        var shared = this.Shared;

        if (shared.Writer != null)
        {
            this.CreateDataPacketHelper = new CreateDataPacketHelper(this.DataSource, shared.DataFormatManager!, this.outputHelper);
            return;
        }

        var security = this.CreateSecurityProvider();
        var config = new StreamingApiConfiguration(
            StreamCreationStrategy.TopicBased,
            this.BrokerUrl,
            [],
            integrateDataFormatManagement: true,
            integrateSessionManagement: false,
            batchingResponses: false,
            domain: this.Domain,
            streamApiPort: ApiPortCreator.CreateNew());

        var client = StreamingApiClientFactory.Create(
            config,
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(security),
            new LoggingDirectoryProvider(""),
            kafkaSecurityConfigProvider: security);

        client.Initialise();
        shared.Writer = client.GetPacketWriterClient();
        shared.Reader = client.GetPacketReaderClient();
        shared.DataFormatManager = client.GetDataFormatManagerClient();
        shared.ConnectionManager = client.GetConnectionManagerClient();
        this.CreateDataPacketHelper = new CreateDataPacketHelper(this.DataSource, shared.DataFormatManager, this.outputHelper);
    }

    private void ConfigureReceiveStream(string sessionKey, DataPacketRequest? request = null)
    {
        var shared = this.Shared;
        if (shared.Reader is null ||
            shared.ConnectionManager is null)
        {
            throw new InvalidOperationException("Clients not initialised.");
        }

        request ??= new DataPacketRequest();
        this.receiveConnection = shared.ConnectionManager.NewConnection(
            new NewConnectionRequest
            {
                Details = new ConnectionDetails
                {
                    DataSource = this.DataSource,
                    SessionKey = sessionKey,
                    StreamOffsets =
                    {
                        0,
                        0
                    },
                    Streams =
                    {
                        this.AppGroup1Stream,
                        this.AppGroup2Stream
                    }
                }
            });
        request.Connection = new Connection
        {
            Id = this.receiveConnection.Connection.Id
        };
        this.receiveStream = shared.Reader.ReadDataPackets(
            new ReadDataPacketsRequest
            {
                Request = request
            });
        this.outputHelper.WriteLine($"Connection ID: {this.receiveConnection.Connection.Id}");
    }

    private async Task WriteDataPacket(WriteDataPacketRequest request, bool log = true)
    {
        if (this.Shared.Writer is null)
        {
            throw new InvalidOperationException("Writer not initialised.");
        }

        var sw = Stopwatch.StartNew();
        await this.Shared.Writer.WriteDataPacketAsync(request);
        sw.Stop();
        if (log)
        {
            this.outputHelper.WriteLine($"publishing time: {sw.ElapsedMilliseconds} ms");
        }
    }

    // -- helpers ---------------------------------------------------------------

    // -- [Fact] tests ----------------------------------------------------------

    [Fact]
    public async Task Publish_One_PeriodicDataPacket_Using_AppGroup1Stream_And_SessionKey_To_AppStream1_Topic()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var runSessionId = Guid.NewGuid().ToString();
        this.ConfigureReceiveStream(runSessionId);
        var periodicDataPacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(["IncludeParameter"]);
        var content = periodicDataPacket.ToByteString();
        var packetType = this.PeriodicDataType;
        var writeRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                Stream = this.AppGroup1Stream,
                DataSource = this.DataSource,
                SessionKey = runSessionId,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                }
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var done = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    this.outputHelper.WriteLine("Received Packet");
                    if (!this.receiveStream.ResponseStream.Current.Response.Any(r =>
                            r.Stream == this.AppGroup1Stream && r.Packet.SessionKey == runSessionId && r.Packet.Type == packetType &&
                            r.Packet.Content.SequenceEqual(content)))
                    {
                        continue;
                    }

                    assertResult = true;
                    done.Set();
                    break;
                }
            },
            token);
        ready.WaitOne(TimeSpan.FromSeconds(5));
        await this.WriteDataPacket(writeRequest);
        done.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_SynchroDataPacket_Using_AppGroup2Stream_And_SessionKey_To_AppStream2_Topic()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var runSessionId = Guid.NewGuid().ToString();
        this.ConfigureReceiveStream(runSessionId);
        var synchroDataPacket = await this.CreateDataPacketHelper.CreateSynchroDataPacket(["IncludeParameter"]);
        var content = synchroDataPacket.ToByteString();
        var packetType = this.SynchroDataType;
        var writeRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                Stream = this.AppGroup2Stream,
                DataSource = this.DataSource,
                SessionKey = runSessionId,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                }
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var done = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    if (!this.receiveStream.ResponseStream.Current.Response.Any(r =>
                            r.Stream == this.AppGroup2Stream && r.Packet.SessionKey == runSessionId && r.Packet.Type == packetType &&
                            r.Packet.Content.SequenceEqual(content)))
                    {
                        continue;
                    }

                    assertResult = true;
                    done.Set();
                    break;
                }
            },
            token);
        ready.Set();
        await this.WriteDataPacket(writeRequest);
        done.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_RowDataPacket_Using_AppGroup2Stream_And_SessionKey_To_AppStream2_Topic()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var runSessionId = Guid.NewGuid().ToString();
        this.ConfigureReceiveStream(runSessionId);
        var rowDataPacket = await this.CreateDataPacketHelper.CreateRowDataPacket(["IncludeParameter"]);
        var content = rowDataPacket.ToByteString();
        var packetType = this.RowDataType;
        var writeRequest = new WriteDataPacketRequest
        {
            Detail = new DataPacketDetails
            {
                Stream = this.AppGroup2Stream,
                DataSource = this.DataSource,
                SessionKey = runSessionId,
                Message = new Packet
                {
                    Content = content,
                    IsEssential = false,
                    SessionKey = runSessionId,
                    Type = packetType
                }
            }
        };
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var done = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    if (!this.receiveStream.ResponseStream.Current.Response.Any(r =>
                            r.Stream == this.AppGroup2Stream && r.Packet.SessionKey == runSessionId && r.Packet.Type == packetType &&
                            r.Packet.Content.SequenceEqual(content)))
                    {
                        continue;
                    }

                    assertResult = true;
                    done.Set();
                    break;
                }
            },
            token);
        ready.Set();
        await this.WriteDataPacket(writeRequest);
        done.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_DataPackets_With_SessionKey_To_Specified_Topic_Based_on_Stream()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var runSessionId = Guid.NewGuid().ToString();
        this.ConfigureReceiveStream(runSessionId);
        var rowDataPacket1 = await this.CreateDataPacketHelper.CreateRowDataPacket(["IncludeParameter"], 1001);
        var rowDataPacket2 = await this.CreateDataPacketHelper.CreateRowDataPacket(["IncludeParameter"], 1002);
        var periodicDataPacket1 = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(["IncludeParameter"], 1003);
        var content1 = rowDataPacket1.ToByteString();
        var content2 = rowDataPacket2.ToByteString();
        var content3 = periodicDataPacket1.ToByteString();
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var e1 = new AutoResetEvent(false);
        var e2 = new AutoResetEvent(false);
        var e3 = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        List<bool> results = [false, false, false];
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    foreach (var r in this.receiveStream.ResponseStream.Current.Response.Where(r => r.Packet.SessionKey == runSessionId))
                    {
                        this.outputHelper.WriteLine("packet received.");
                        if (r.Stream == this.AppGroup1Stream &&
                            r.Packet.Content.SequenceEqual(content1))
                        {
                            results[0] = true;
                            e1.Set();
                        }
                        else if (r.Stream == this.AppGroup2Stream &&
                                 r.Packet.Content.SequenceEqual(content2))
                        {
                            results[1] = true;
                            e2.Set();
                        }
                        else if (r.Stream == "" &&
                                 r.Packet.Content.SequenceEqual(content3))
                        {
                            results[2] = true;
                            e3.Set();
                        }
                    }

                    if (results[0] &&
                        results[1] &&
                        results[2])
                    {
                        break;
                    }
                }
            },
            token);
        ready.WaitOne(TimeSpan.FromSeconds(5));
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = content1,
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.RowDataType
                    }
                }
            });
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup2Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = content2,
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.RowDataType
                    }
                }
            });
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = content3,
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.PeriodicDataType
                    }
                }
            });
        e1.WaitOne(TimeSpan.FromSeconds(10));
        e2.WaitOne(TimeSpan.FromSeconds(10));
        e3.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        results.ForEach(r => r.Should().BeTrue());
    }

    [Fact]
    public async Task When_Publish_Data_With_Different_SessionKey_Than_The_Receive_SessionKey_The_Data_Should_Net_Be_Delivered()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var runSessionId = Guid.NewGuid().ToString();
        this.ConfigureReceiveStream(runSessionId);
        var periodicDataPacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(["IncludeParameter"], 4001);
        var content = periodicDataPacket.ToByteString();
        const string UnknownSessionKey = "unknown";
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var done = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.SessionKey == UnknownSessionKey))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.SessionKey == runSessionId))
                    {
                        done.Set();
                    }
                }
            },
            token);
        ready.WaitOne(TimeSpan.FromSeconds(5));
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Message = new Packet
                    {
                        Content = content,
                        IsEssential = false,
                        SessionKey = UnknownSessionKey,
                        Type = this.PeriodicDataType
                    },
                    SessionKey = UnknownSessionKey
                }
            },
            log: false);
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    Message = new Packet
                    {
                        Content = content,
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.PeriodicDataType
                    },
                    SessionKey = runSessionId
                }
            },
            log: false);
        done.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task When_Publish_1000_Data_Using_WriteStream_And_SessionKey_All_Messages_Should_Be_Delivered()
    {
        if (this.Shared.Writer is null ||
            this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        var runSessionId = Guid.NewGuid().ToString();
        this.ConfigureReceiveStream(runSessionId);
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        var writeStream = this.Shared.Writer.WriteDataPackets();
        var rnd = new Random();
        const int N = 1000;
        var requests = new List<WriteDataPacketsRequest>(N);
        for (var i = 0; i < N; i++)
        {
            var periodicDataPacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(["IncludeParameter"], rnd.Next());
            var content = periodicDataPacket.ToByteString();
            requests.Add(
                new WriteDataPacketsRequest
                {
                    Details =
                    {
                        new DataPacketDetails
                        {
                            Stream = i % 3 == 0 ? "" : (i % 3 == 1 ? this.AppGroup1Stream : this.AppGroup2Stream),
                            DataSource = this.DataSource,
                            SessionKey = runSessionId,
                            Message = new Packet
                            {
                                Content = content,
                                IsEssential = false,
                                SessionKey = runSessionId,
                                Type = this.PeriodicDataType
                            }
                        }
                    }
                });
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var done = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        var receiveCount = 0;
        var sw = new Stopwatch();
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    foreach (var _ in this.receiveStream.ResponseStream.Current.Response.Where(i => i.Packet.SessionKey == runSessionId))
                    {
                        if (receiveCount == 0)
                        {
                            sw.Start();
                        }

                        receiveCount++;
                    }

                    if (receiveCount != N)
                    {
                        continue;
                    }

                    this.outputHelper.WriteLine($"receivingTime : {sw.ElapsedMilliseconds} ms");
                    done.Set();
                    break;
                }
            },
            token);
        ready.WaitOne(TimeSpan.FromSeconds(5));
        var publishSw = Stopwatch.StartNew();
        foreach (var req in requests)
        {
            await writeStream.RequestStream.WriteAsync(req, token);
        }

        publishSw.Stop();
        this.outputHelper.WriteLine($"publishing time: {publishSw.ElapsedMilliseconds} ms");
        done.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        receiveCount.Should().Be(N);
    }

    [Fact]
    public async Task Publish_One_EventPacket_Using_AppGroup1Stream_And_SessionKey_To_AppStream1_Topic()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var runSessionId = Guid.NewGuid().ToString();
        this.ConfigureReceiveStream(runSessionId);
        var eventPacket = await this.CreateDataPacketHelper.CreateEventPacket("IncludeEvent");
        var content = eventPacket.ToByteString();
        var packetType = this.EventType;
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var done = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    this.outputHelper.WriteLine("Received Packet");
                    if (!this.receiveStream.ResponseStream.Current.Response.Any(r =>
                            r.Stream == this.AppGroup1Stream && r.Packet.SessionKey == runSessionId && r.Packet.Type == packetType &&
                            r.Packet.Content.SequenceEqual(content)))
                    {
                        continue;
                    }

                    assertResult = true;
                    done.Set();
                    break;
                }
            },
            token);
        ready.WaitOne(TimeSpan.FromSeconds(5));
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    Stream = this.AppGroup1Stream,
                    DataSource = this.DataSource,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = content,
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = packetType
                    }
                }
            });
        done.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_MarkerPacket_Using_AppGroup1Stream_And_SessionKey_To_AppStream1_Topic()
    {
        var runSessionId = Guid.NewGuid().ToString();
        this.ConfigureReceiveStream(
            runSessionId,
            new DataPacketRequest
            {
                IncludeMarkers = true
            });
        var markerPacket = new MarkerPacket
        {
            Timestamp = 1732898780000123456,
            Label = "Test Marker",
            Type = "Lap",
            Value = 1
        };
        var content = markerPacket.ToByteString();
        var packetType = this.MarkerType;
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var done = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    this.outputHelper.WriteLine("Received Packet");
                    if (!this.receiveStream.ResponseStream.Current.Response.Any(r =>
                            r.Stream == this.AppGroup1Stream && r.Packet.SessionKey == runSessionId && r.Packet.Type == packetType &&
                            r.Packet.Content.SequenceEqual(content)))
                    {
                        continue;
                    }

                    assertResult = true;
                    done.Set();
                    break;
                }
            },
            token);
        ready.WaitOne(TimeSpan.FromSeconds(5));
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    Stream = this.AppGroup1Stream,
                    DataSource = this.DataSource,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = content,
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = packetType
                    }
                }
            });
        done.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_MarkerPacket_And_Not_Be_Delivered_When_IncludeMarkers_Is_Set_To_False()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var runSessionId = Guid.NewGuid().ToString();
        this.ConfigureReceiveStream(
            runSessionId,
            new DataPacketRequest
            {
                IncludeMarkers = false
            });
        var periodicDataPacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(["IncludeParameter"], 4001);
        var markerContent = new MarkerPacket
        {
            Timestamp = 1732898780000123456,
            Label = "Test Marker",
            Type = "Lap",
            Value = 1
        }.ToByteString();
        var periodicContent = periodicDataPacket.ToByteString();
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var done = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Type == this.MarkerType))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.SessionKey == runSessionId))
                    {
                        done.Set();
                    }
                }
            },
            token);
        ready.WaitOne(TimeSpan.FromSeconds(5));
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = markerContent,
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.MarkerType
                    }
                }
            },
            log: false);
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = periodicContent,
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.PeriodicDataType
                    }
                }
            },
            log: false);
        done.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        receiveCount.Should().Be(0);
    }

    private async Task RunFilterTest(
        string runSessionId,
        DataPacketRequest dataPacketRequest,
        ByteString includeContent,
        ByteString excludeContent,
        WriteDataPacketRequest writeIncludePacketRequest,
        WriteDataPacketRequest writeExcludePacketRequest)
    {
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var done = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        var receiveCount = 0;
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                if (await this.receiveStream.ResponseStream.MoveNext())
                {
                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(excludeContent)))
                    {
                        receiveCount++;
                    }

                    if (this.receiveStream.ResponseStream.Current.Response.Any(i => i.Packet.Content.SequenceEqual(includeContent)))
                    {
                        done.Set();
                    }
                }
            },
            token);
        ready.WaitOne(TimeSpan.FromSeconds(5));
        await this.WriteDataPacket(writeExcludePacketRequest);
        await this.WriteDataPacket(writeIncludePacketRequest);
        done.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task Only_Deliver_Periodic_Packets_Filtered_By_IncludeParameters()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var includeParameters = new List<string>
        {
            "IncludeParameter123"
        };
        var excludeParameters = new List<string>
        {
            "ExcludeParameter123"
        };
        var runSessionId = Guid.NewGuid().ToString();
        var req = new DataPacketRequest();
        req.IncludeParameters.Add(includeParameters);
        this.ConfigureReceiveStream(runSessionId, req);
        var includePacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(includeParameters, 4001);
        var excludePacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(excludeParameters, 4002);
        await this.RunFilterTest(
            runSessionId,
            req,
            includePacket.ToByteString(),
            excludePacket.ToByteString(),
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = includePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.PeriodicDataType
                    }
                }
            },
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = excludePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.PeriodicDataType
                    }
                }
            });
    }

    [Fact]
    public async Task Only_Deliver_Row_Packets_Filtered_By_IncludeParameters()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var includeParameters = new List<string>
        {
            "IncludeParameter123"
        };
        var excludeParameters = new List<string>
        {
            "ExcludeParameter123"
        };
        var runSessionId = Guid.NewGuid().ToString();
        var req = new DataPacketRequest();
        req.IncludeParameters.Add(includeParameters);
        this.ConfigureReceiveStream(runSessionId, req);
        var includePacket = await this.CreateDataPacketHelper.CreateRowDataPacket(includeParameters, 4001);
        var excludePacket = await this.CreateDataPacketHelper.CreateRowDataPacket(excludeParameters, 4002);
        await this.RunFilterTest(
            runSessionId,
            req,
            includePacket.ToByteString(),
            excludePacket.ToByteString(),
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = includePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.RowDataType
                    }
                }
            },
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = excludePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.RowDataType
                    }
                }
            });
    }

    [Fact]
    public async Task Only_Deliver_Synchro_Packets_Filtered_By_IncludeParameters()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var includeParameters = new List<string>
        {
            "IncludeParameter123"
        };
        var excludeParameters = new List<string>
        {
            "ExcludeParameter123"
        };
        var runSessionId = Guid.NewGuid().ToString();
        var req = new DataPacketRequest();
        req.IncludeParameters.Add(includeParameters);
        this.ConfigureReceiveStream(runSessionId, req);
        var includePacket = await this.CreateDataPacketHelper.CreateSynchroDataPacket(includeParameters, 4001);
        var excludePacket = await this.CreateDataPacketHelper.CreateSynchroDataPacket(excludeParameters, 4002);
        await this.RunFilterTest(
            runSessionId,
            req,
            includePacket.ToByteString(),
            excludePacket.ToByteString(),
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = includePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.SynchroDataType
                    }
                }
            },
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = excludePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.SynchroDataType
                    }
                }
            });
    }

    [Fact]
    public async Task Only_Deliver_Periodic_Packets_Not_Filtered_By_ExcludeParameters()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var includeParameters = new List<string>
        {
            "IncludeParameter123"
        };
        var excludeParameters = new List<string>
        {
            "ExcludeParameter123"
        };
        var runSessionId = Guid.NewGuid().ToString();
        var req = new DataPacketRequest();
        req.ExcludeParameters.Add(excludeParameters);
        this.ConfigureReceiveStream(runSessionId, req);
        var includePacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(includeParameters, 4001);
        var excludePacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(excludeParameters, 4002);
        await this.RunFilterTest(
            runSessionId,
            req,
            includePacket.ToByteString(),
            excludePacket.ToByteString(),
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = includePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.PeriodicDataType
                    }
                }
            },
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = excludePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.PeriodicDataType
                    }
                }
            });
    }

    [Fact]
    public async Task Only_Deliver_Row_Packets_Not_Filtered_By_ExcludeParameters()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var includeParameters = new List<string>
        {
            "IncludeParameter123"
        };
        var excludeParameters = new List<string>
        {
            "ExcludeParameter123"
        };
        var runSessionId = Guid.NewGuid().ToString();
        var req = new DataPacketRequest();
        req.ExcludeParameters.Add(excludeParameters);
        this.ConfigureReceiveStream(runSessionId, req);
        var includePacket = await this.CreateDataPacketHelper.CreateRowDataPacket(includeParameters, 4001);
        var excludePacket = await this.CreateDataPacketHelper.CreateRowDataPacket(excludeParameters, 4002);
        await this.RunFilterTest(
            runSessionId,
            req,
            includePacket.ToByteString(),
            excludePacket.ToByteString(),
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = includePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.RowDataType
                    }
                }
            },
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = excludePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.RowDataType
                    }
                }
            });
    }

    [Fact]
    public async Task Only_Deliver_Synchro_Packets_Not_Filtered_By_ExcludeParameters()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var includeParameters = new List<string>
        {
            "IncludeParameter123"
        };
        var excludeParameters = new List<string>
        {
            "ExcludeParameter123"
        };
        var runSessionId = Guid.NewGuid().ToString();
        var req = new DataPacketRequest();
        req.ExcludeParameters.Add(excludeParameters);
        this.ConfigureReceiveStream(runSessionId, req);
        var includePacket = await this.CreateDataPacketHelper.CreateSynchroDataPacket(includeParameters, 4001);
        var excludePacket = await this.CreateDataPacketHelper.CreateSynchroDataPacket(excludeParameters, 4002);
        await this.RunFilterTest(
            runSessionId,
            req,
            includePacket.ToByteString(),
            excludePacket.ToByteString(),
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = includePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.SynchroDataType
                    }
                }
            },
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = excludePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.SynchroDataType
                    }
                }
            });
    }

    [Fact]
    public async Task Only_Deliver_Event_Packets_Filtered_By_IncludeEvents()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var includeEvents = "IncludeEvents321";
        var excludeEvents = "ExcludeEvents321";
        var runSessionId = Guid.NewGuid().ToString();
        var req = new DataPacketRequest();
        req.IncludeEvents.Add(includeEvents);
        this.ConfigureReceiveStream(runSessionId, req);
        var includePacket = await this.CreateDataPacketHelper.CreateEventPacket(includeEvents);
        var excludePacket = await this.CreateDataPacketHelper.CreateEventPacket(excludeEvents);
        await this.RunFilterTest(
            runSessionId,
            req,
            includePacket.ToByteString(),
            excludePacket.ToByteString(),
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = includePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.EventType
                    }
                }
            },
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = excludePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.EventType
                    }
                }
            });
    }

    [Fact]
    public async Task Only_Deliver_Event_Packets_Not_Filtered_By_ExcludeEvents()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var includeEvents = "IncludeEvents321";
        var excludeEvents = "ExcludeEvents321";
        var runSessionId = Guid.NewGuid().ToString();
        var req = new DataPacketRequest();
        req.ExcludeEvents.Add(excludeEvents);
        this.ConfigureReceiveStream(runSessionId, req);
        var includePacket = await this.CreateDataPacketHelper.CreateEventPacket(includeEvents);
        var excludePacket = await this.CreateDataPacketHelper.CreateEventPacket(excludeEvents);
        await this.RunFilterTest(
            runSessionId,
            req,
            includePacket.ToByteString(),
            excludePacket.ToByteString(),
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = includePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.EventType
                    }
                }
            },
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = excludePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.EventType
                    }
                }
            });
    }

    [Fact]
    public async Task Prioritising_Exclude_List_And_Deliver_Periodic_Packets_Filtered()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var includeParameters = new List<string>
        {
            "IncludeParameter123"
        };
        var excludeParameters = new List<string>
        {
            "ExcludeParameter123"
        };
        var runSessionId = Guid.NewGuid().ToString();
        var req = new DataPacketRequest();
        req.IncludeParameters.Add(includeParameters);
        req.IncludeParameters.Add(excludeParameters);
        req.ExcludeParameters.Add(excludeParameters);
        this.ConfigureReceiveStream(runSessionId, req);
        var includePacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(includeParameters, 4001);
        var excludePacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(excludeParameters, 4002);
        await this.RunFilterTest(
            runSessionId,
            req,
            includePacket.ToByteString(),
            excludePacket.ToByteString(),
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = includePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.PeriodicDataType
                    }
                }
            },
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = excludePacket.ToByteString(),
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.PeriodicDataType
                    }
                }
            });
    }

    [Fact]
    public async Task Still_Include_Periodic_Packet_If_Parameters_In_Both_Include_And_Exclude_List()
    {
        if (this.CreateDataPacketHelper is null)
        {
            throw new InvalidOperationException("helper null");
        }

        var includeParameters = new List<string>
        {
            "IncludeParameter123"
        };
        var excludeParameters = new List<string>
        {
            "ExcludeParameter123"
        };
        var packetParameters = new List<string>();
        packetParameters.AddRange(includeParameters);
        packetParameters.AddRange(excludeParameters);
        var runSessionId = Guid.NewGuid().ToString();
        var req = new DataPacketRequest();
        req.IncludeParameters.Add(includeParameters);
        req.ExcludeParameters.Add(excludeParameters);
        this.ConfigureReceiveStream(runSessionId, req);
        var includePacket = await this.CreateDataPacketHelper.CreatePeriodicDataPacket(packetParameters, 4001);
        var includeContent = includePacket.ToByteString();
        if (this.receiveStream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        using var cts = new CancellationTokenSource();
        var token = cts.Token;
        var done = new AutoResetEvent(false);
        var ready = new AutoResetEvent(false);
        var assertResult = false;
        _ = Task.Run(
            async () =>
            {
                ready.Set();
                while (await this.receiveStream.ResponseStream.MoveNext(token))
                {
                    this.outputHelper.WriteLine("Received Packet");
                    if (!this.receiveStream.ResponseStream.Current.Response.Any(r =>
                            r.Stream == this.AppGroup1Stream && r.Packet.SessionKey == runSessionId && r.Packet.Type == this.PeriodicDataType &&
                            r.Packet.Content.SequenceEqual(includeContent)))
                    {
                        continue;
                    }

                    assertResult = true;
                    done.Set();
                    break;
                }
            },
            token);
        ready.WaitOne(TimeSpan.FromSeconds(5));
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    Stream = this.AppGroup1Stream,
                    SessionKey = runSessionId,
                    Message = new Packet
                    {
                        Content = includeContent,
                        IsEssential = false,
                        SessionKey = runSessionId,
                        Type = this.PeriodicDataType
                    }
                }
            });
        done.WaitOne(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        assertResult.Should().BeTrue();
    }
}
