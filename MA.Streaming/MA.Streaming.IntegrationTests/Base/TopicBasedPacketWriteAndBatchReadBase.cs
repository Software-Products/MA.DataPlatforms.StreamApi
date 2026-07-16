// <copyright file="TopicBasedPacketWriteAndBatchReadBase.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Core;
using MA.Streaming.Core.Configs;
using MA.Streaming.Core.Routing;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Client.Local;
using MA.Streaming.Proto.ServerComponent;

using NSubstitute;

using Xunit;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests.Base;

public abstract class TopicBasedPacketWriteAndBatchReadBase
{
    private sealed class TestContext
    {
        public StreamingApiClient? Client;
        public PacketWriterService.PacketWriterServiceClient? Writer;
        public PacketReaderService.PacketReaderServiceClient? Reader;
        public string SessionKey = "";
        public NewConnectionResponse? Connection;
        public AsyncServerStreamingCall<ReadPacketsResponse>? Stream;
    }

    private static readonly ConcurrentDictionary<Type, TestContext> Contexts = new();

    private TestContext Ctx => Contexts.GetOrAdd(this.GetType(), _ => new TestContext());

    protected abstract string BrokerUrl { get; }

    protected abstract string DataSource { get; }

    protected abstract string Domain { get; }

    protected abstract string SessionKeyPrefix { get; }

    protected virtual string AppGroup1Stream => "AppGroup1Stream";

    protected virtual string AppGroup2Stream => "AppGroup2Stream";

    protected virtual string SampleType => "SampleType";

    protected virtual IKafkaSecurityConfigurationProvider CreateSecurityProvider() =>
        new EmptyKafkaSecurityConfigProvider();

    private readonly ITestOutputHelper outputHelper;
    private readonly ILogger logger = Substitute.For<ILogger>();

    protected TopicBasedPacketWriteAndBatchReadBase(ITestOutputHelper outputHelper)
    {
        this.outputHelper = outputHelper;
        this.EnsureInitialised();
    }

    private void EnsureInitialised()
    {
        if (this.Ctx.Client != null)
        {
            this.RefreshResponseStream();
            return;
        }

        var security = this.CreateSecurityProvider();
        var config = new StreamingApiConfiguration(
            StreamCreationStrategy.TopicBased,
            this.BrokerUrl,
            [],
            integrateDataFormatManagement: false,
            integrateSessionManagement: false,
            batchingResponses: true,
            streamApiPort: ApiPortCreator.CreateNew(),
            domain: this.Domain);

        var client = StreamingApiClientFactory.Create(
            config,
            new CancellationTokenSourceProvider(),
            new KafkaBrokerAvailabilityChecker(security),
            new LoggingDirectoryProvider(""),
            kafkaSecurityConfigProvider: security);

        client.Initialise();
        this.Ctx.Client = client;
        this.Ctx.Writer = client.GetPacketWriterClient();
        this.Ctx.Reader = client.GetPacketReaderClient();
        this.RefreshResponseStream();
    }

    private void RefreshResponseStream()
    {
        var ctx = this.Ctx;
        if (ctx.Client is null ||
            ctx.Reader is null)
        {
            throw new InvalidOperationException("StreamingApiClient did not initialise properly.");
        }

        ctx.SessionKey = SessionCreator.Create(this.SessionKeyPrefix);
        ctx.Connection = ctx.Client.GetConnectionManagerClient().NewConnection(
            new NewConnectionRequest
            {
                Details = new ConnectionDetails
                {
                    DataSource = this.DataSource,
                    SessionKey = ctx.SessionKey,
                    StreamOffsets =
                    {
                        0,
                        0
                    },
                    Streams =
                    {
                        this.AppGroup1Stream,
                        this.AppGroup2Stream
                    },
                    ExcludeMainStream = false
                }
            });
        ctx.Stream = ctx.Reader.ReadPackets(
            new ReadPacketsRequest
            {
                Connection = new Connection
                {
                    Id = ctx.Connection.Connection.Id
                }
            });
    }

    private async Task WriteDataPacket(WriteDataPacketRequest request, bool log = true)
    {
        if (this.Ctx.Writer is null)
        {
            throw new InvalidOperationException("PacketWriter not initialised.");
        }

        var sw = Stopwatch.StartNew();
        await this.Ctx.Writer.WriteDataPacketAsync(request);
        sw.Stop();
        if (log)
        {
            this.outputHelper.WriteLine($"publishing time: {sw.ElapsedMilliseconds} ms");
        }
    }

    [Fact]
    public async Task Publish_One_DataPacket_Using_AppGroup1Stream_And_SessionKey_To_AppStream1_Topic()
    {
        var ctx = this.Ctx;
        if (ctx.Stream is null)
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
                while (await ctx.Stream.ResponseStream.MoveNext(token))
                {
                    if (!ctx.Stream.ResponseStream.Current.Response.Any(r => r.Stream == this.AppGroup1Stream && r.Packet.Type == this.SampleType))
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
                    SessionKey = ctx.SessionKey,
                    Message = new Packet
                    {
                        Content = ByteString.CopyFrom([1, 2, 3]),
                        IsEssential = false,
                        SessionKey = ctx.SessionKey,
                        Type = this.SampleType
                    }
                }
            });
        done.WaitOne(TimeSpan.FromSeconds(60));
        await cts.CancelAsync();
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_DataPacket_Using_AppGroup2Stream_And_SessionKey_To_AppStream2_Topic()
    {
        var ctx = this.Ctx;
        if (ctx.Stream is null)
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
                while (await ctx.Stream.ResponseStream.MoveNext(token))
                {
                    if (!ctx.Stream.ResponseStream.Current.Response.Any(r => r.Stream == this.AppGroup2Stream && r.Packet.Type == this.SampleType))
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
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    Stream = this.AppGroup2Stream,
                    DataSource = this.DataSource,
                    SessionKey = ctx.SessionKey,
                    Message = new Packet
                    {
                        Content = ByteString.CopyFrom([4, 5, 6]),
                        IsEssential = false,
                        SessionKey = ctx.SessionKey,
                        Type = this.SampleType
                    }
                }
            });
        done.WaitOne(TimeSpan.FromSeconds(60));
        await cts.CancelAsync();
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_One_DataPacket_Using_Empty_Stream_And_SessionKey_To_MainDataSource_Topic()
    {
        var ctx = this.Ctx;
        if (ctx.Stream is null)
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
                while (await ctx.Stream.ResponseStream.MoveNext(token))
                {
                    if (!ctx.Stream.ResponseStream.Current.Response.Any(r => string.IsNullOrEmpty(r.Stream) && r.Packet.Type == this.SampleType))
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
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    Stream = "",
                    DataSource = this.DataSource,
                    SessionKey = ctx.SessionKey,
                    Message = new Packet
                    {
                        Content = ByteString.CopyFrom([4, 5, 6]),
                        IsEssential = false,
                        SessionKey = ctx.SessionKey,
                        Type = this.SampleType
                    }
                }
            });
        done.WaitOne(TimeSpan.FromSeconds(60));
        await cts.CancelAsync();
        assertResult.Should().BeTrue();
    }

    [Fact]
    public async Task Publish_DataPackets_With_SessionKey_To_Specified_Topic_Based_on_Stream()
    {
        var ctx = this.Ctx;
        if (ctx.Stream is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        var c1 = ByteString.CopyFrom([13, 14, 15]);
        var c2 = ByteString.CopyFrom([16, 17, 18]);
        var c3 = ByteString.CopyFrom([19, 20, 21]);
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
                while (await ctx.Stream.ResponseStream.MoveNext(token))
                {
                    foreach (var r in ctx.Stream.ResponseStream.Current.Response)
                    {
                        if (r.Stream == this.AppGroup1Stream &&
                            r.Packet.Content.SequenceEqual(c1))
                        {
                            results[0] = true;
                            e1.Set();
                        }
                        else if (r.Stream == this.AppGroup2Stream &&
                                 r.Packet.Content.SequenceEqual(c2))
                        {
                            results[1] = true;
                            e2.Set();
                        }
                        else if (r.Stream == "" &&
                                 r.Packet.Content.SequenceEqual(c3))
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
                    SessionKey = ctx.SessionKey,
                    Message = new Packet
                    {
                        Content = c1,
                        IsEssential = false,
                        SessionKey = ctx.SessionKey,
                        Type = this.SampleType
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
                    SessionKey = ctx.SessionKey,
                    Message = new Packet
                    {
                        Content = c2,
                        IsEssential = false,
                        SessionKey = ctx.SessionKey,
                        Type = this.SampleType
                    }
                }
            });
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    DataSource = this.DataSource,
                    SessionKey = ctx.SessionKey,
                    Message = new Packet
                    {
                        Content = c3,
                        IsEssential = false,
                        SessionKey = ctx.SessionKey,
                        Type = this.SampleType
                    }
                }
            });
        e1.WaitOne(TimeSpan.FromSeconds(60));
        e2.WaitOne(TimeSpan.FromSeconds(60));
        e3.WaitOne(TimeSpan.FromSeconds(60));
        await cts.CancelAsync();
        results.ForEach(r => r.Should().BeTrue());
    }

    [Fact]
    public async Task When_Publish_Data_With_Different_SessionKey_Than_The_Receive_SessionKey_The_Data_Should_Net_Be_Delivered()
    {
        var ctx = this.Ctx;
        const string UnknownSessionKey = "unknown";
        if (ctx.Stream is null)
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
                if (await ctx.Stream.ResponseStream.MoveNext())
                {
                    if (ctx.Stream.ResponseStream.Current.Response.Any(i => i.Packet.SessionKey == UnknownSessionKey))
                    {
                        receiveCount++;
                    }

                    if (ctx.Stream.ResponseStream.Current.Response.Any(i => i.Packet.SessionKey == ctx.SessionKey))
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
                        Content = ByteString.CopyFrom([22, 23, 24]),
                        IsEssential = false,
                        SessionKey = UnknownSessionKey,
                        Type = this.SampleType
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
                        Content = ByteString.CopyFrom([22, 23, 24]),
                        IsEssential = false,
                        SessionKey = ctx.SessionKey,
                        Type = this.SampleType
                    },
                    SessionKey = ctx.SessionKey
                }
            },
            log: false);
        done.WaitOne(TimeSpan.FromSeconds(60));
        await cts.CancelAsync();
        receiveCount.Should().Be(0);
    }

    [Fact]
    public async Task When_Publish_1000_Data_Using_WriteStream_And_SessionKey_All_Messages_Should_Be_Delivered()
    {
        var ctx = this.Ctx;
        if (ctx.Stream is null ||
            ctx.Writer is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        var writeStream = ctx.Writer.WriteDataPackets();
        var rnd = new Random();
        const int N = 1000;
        var expected = N + 3;
        var requests = Enumerable.Range(0, N).Select(i =>
        {
            var b = new byte[3];
            rnd.NextBytes(b);
            return new WriteDataPacketsRequest
            {
                Details =
                {
                    new DataPacketDetails
                    {
                        Stream = i % 3 == 0 ? "" : (i % 3 == 1 ? this.AppGroup1Stream : this.AppGroup2Stream),
                        DataSource = this.DataSource,
                        SessionKey = ctx.SessionKey,
                        Message = new Packet
                        {
                            Content = ByteString.CopyFrom(b),
                            IsEssential = false,
                            SessionKey = ctx.SessionKey,
                            Type = i.ToString()
                        }
                    }
                }
            };
        }).ToList();
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
                while (await ctx.Stream.ResponseStream.MoveNext(token))
                {
                    foreach (var _ in ctx.Stream.ResponseStream.Current.Response)
                    {
                        if (receiveCount == 0)
                        {
                            sw.Start();
                        }

                        receiveCount++;
                    }

                    if (receiveCount != expected)
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
        done.WaitOne(TimeSpan.FromSeconds(60));
        await cts.CancelAsync();
        receiveCount.Should().Be(expected);
    }

    [Fact]
    public async Task Publish_One_Essential_DataPacket_Read_All_Essentials_Should_Deliver_It_And_Packet_Should_Be_Received_On_Defined_DataSource_Stream_Too()
    {
        var ctx = this.Ctx;
        if (ctx.Stream is null ||
            ctx.Reader is null ||
            ctx.Connection is null)
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
                while (await ctx.Stream.ResponseStream.MoveNext(token))
                {
                    if (!ctx.Stream.ResponseStream.Current.Response.Any(r =>
                            r.Stream == this.AppGroup1Stream && r.Packet.Type == this.SampleType && r.Packet.SessionKey == ctx.SessionKey && r.Packet.IsEssential))
                    {
                        continue;
                    }

                    assertResult = true;
                    done.Set();
                    break;
                }
            },
            token);
        ready.WaitOne(TimeSpan.FromSeconds(60));
        await this.WriteDataPacket(
            new WriteDataPacketRequest
            {
                Detail = new DataPacketDetails
                {
                    Stream = this.AppGroup1Stream,
                    DataSource = this.DataSource,
                    SessionKey = ctx.SessionKey,
                    Message = new Packet
                    {
                        Content = ByteString.CopyFrom([1, 2, 3]),
                        IsEssential = true,
                        SessionKey = ctx.SessionKey,
                        Type = this.SampleType
                    }
                }
            });
        done.WaitOne(TimeSpan.FromSeconds(60));
        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(1));
        var lstResult = new List<ReadEssentialsResponse>();
        var essResult = ctx.Reader.ReadEssentials(
            new ReadEssentialsRequest
            {
                Connection = ctx.Connection.Connection
            });
        var essEvent = new AutoResetEvent(false);
        var counter = 0;
        _ = Task.Run(
            async () =>
            {
                while (await essResult.ResponseStream.MoveNext())
                {
                    lstResult.Add(essResult.ResponseStream.Current);
                    counter += essResult.ResponseStream.Current.Response.Count(i => i.Packet.SessionKey == ctx.SessionKey);
                    if (counter == 2)
                    {
                        essEvent.Set();
                    }
                }
            },
            token);
        essEvent.WaitOne(TimeSpan.FromSeconds(60));
        await cts.CancelAsync();
        assertResult.Should().BeTrue();
        lstResult.Sum(i => i.Response.Count(j => j.Packet.SessionKey == ctx.SessionKey)).Should().Be(2);
    }

    [Fact]
    public async Task Publish_1000_Essential_DataPacket_Read_All_Essentials_Should_Deliver_All_And_Packet_Should_Be_Received_On_Defined_DataSource_Stream_Too()
    {
        var ctx = this.Ctx;
        if (ctx.Stream is null ||
            ctx.Reader is null ||
            ctx.Connection is null ||
            ctx.Writer is null ||
            ctx.Client is null)
        {
            throw new InvalidOperationException("test not initialised properly");
        }

        var writeStream = ctx.Writer.WriteDataPackets();
        var rnd = new Random();
        const int N = 1000;
        var expected = 1003;
        var requests = Enumerable.Range(0, N).Select(i =>
        {
            var b = new byte[3];
            rnd.NextBytes(b);
            return new WriteDataPacketsRequest
            {
                Details =
                {
                    new DataPacketDetails
                    {
                        Stream = i % 3 == 0 ? "" : (i % 3 == 1 ? this.AppGroup1Stream : this.AppGroup2Stream),
                        DataSource = this.DataSource,
                        SessionKey = ctx.SessionKey,
                        Message = new Packet
                        {
                            Content = ByteString.CopyFrom(b),
                            IsEssential = true,
                            SessionKey = ctx.SessionKey,
                            Type = i.ToString()
                        }
                    }
                }
            };
        }).ToList();
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
                while (await ctx.Stream.ResponseStream.MoveNext(token))
                {
                    foreach (var _ in ctx.Stream.ResponseStream.Current.Response.Where(i => i.Packet.SessionKey == ctx.SessionKey))
                    {
                        if (receiveCount == 0)
                        {
                            sw.Start();
                        }

                        receiveCount++;
                    }

                    if (receiveCount != expected)
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
        done.WaitOne(TimeSpan.FromSeconds(60));
        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(5));
        var newConn = await ctx.Client.GetConnectionManagerClient().NewConnectionAsync(
            new NewConnectionRequest
            {
                Details = new ConnectionDetails
                {
                    DataSource = this.DataSource,
                    SessionKey = ctx.SessionKey,
                    StreamOffsets =
                    {
                        0,
                        0
                    },
                    Streams =
                    {
                        this.AppGroup1Stream,
                        this.AppGroup2Stream
                    },
                    ExcludeMainStream = false
                }
            });
        var essResult = ctx.Reader.ReadEssentials(
            new ReadEssentialsRequest
            {
                Connection = newConn.Connection
            });
        var lstResult = new List<ReadEssentialsResponse>();
        var essEvent = new AutoResetEvent(false);
        var counter = 0;
        _ = Task.Run(
            async () =>
            {
                while (await essResult.ResponseStream.MoveNext())
                {
                    lstResult.Add(essResult.ResponseStream.Current);
                    counter += essResult.ResponseStream.Current.Response.Count(i => i.Packet.SessionKey == ctx.SessionKey);
                    if (counter == N + 1)
                    {
                        essEvent.Set();
                    }
                }
            },
            token);
        essEvent.WaitOne(TimeSpan.FromSeconds(60));
        await cts.CancelAsync();
        receiveCount.Should().Be(expected);
        lstResult.Sum(i => i.Response.Count(j => j.Packet.SessionKey == ctx.SessionKey)).Should().Be(1001);
    }

    [Fact]
    public async Task When_Publish_1_000_000_Data_Using_WriteStream_And_SessionKey_All_Messages_Should_Be_Delivered()
    {
        await HeavyTestGate.Semaphore.WaitAsync();
        try
        {
            var ctx = this.Ctx;
            if (ctx.Stream is null ||
                ctx.Writer is null)
            {
                throw new InvalidOperationException("test not initialised properly");
            }

            var writeStream = ctx.Writer.WriteDataPackets();
            using var cts = new CancellationTokenSource();
            var token = cts.Token;
            var batchPublisher = new BatchPublisher(this.logger, writeStream, token);
            var rnd = new Random();
            const int N = 1_000_000;
            const int Expected = N + 3;
            var details = Enumerable.Range(0, N).Select(i =>
            {
                var b = new byte[3];
                rnd.NextBytes(b);
                return new DataPacketDetails
                {
                    Stream = i % 3 == 0 ? "" : (i % 3 == 1 ? this.AppGroup1Stream : this.AppGroup2Stream),
                    DataSource = this.DataSource,
                    SessionKey = ctx.SessionKey,
                    Message = new Packet
                    {
                        Content = ByteString.CopyFrom(b),
                        IsEssential = false,
                        SessionKey = ctx.SessionKey,
                        Type = i.ToString()
                    }
                };
            }).ToList();
            var done = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var receiveCount = 0;
            var sw = new Stopwatch();
            var publishDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var publishSw = Stopwatch.StartNew();
            batchPublisher.DataPacketPublishCompleted += (_, _) =>
            {
                publishSw.Stop();
                this.outputHelper.WriteLine($"publishing time: {publishSw.ElapsedMilliseconds} ms");
                publishDone.TrySetResult(true);
            };
            foreach (var d in details)
            {
                batchPublisher.Add(d);
            }

            batchPublisher.Finish();
            await Task.WhenAny(publishDone.Task, Task.Delay(TimeSpan.FromMinutes(5)));
            _ = Task.Run(
                async () =>
                {
                    while (await ctx.Stream.ResponseStream.MoveNext(token))
                    {
                        foreach (var _ in ctx.Stream.ResponseStream.Current.Response)
                        {
                            if (receiveCount == 0)
                            {
                                sw.Start();
                            }

                            receiveCount++;
                        }

                        if (receiveCount != Expected)
                        {
                            continue;
                        }

                        this.outputHelper.WriteLine($"receivingTime : {sw.ElapsedMilliseconds} ms");
                        sw.Stop();
                        done.TrySetResult(true);
                        break;
                    }
                },
                token);
            await Task.WhenAny(done.Task, Task.Delay(TimeSpan.FromMinutes(5)));
            await cts.CancelAsync();
            receiveCount.Should().Be(Expected);
        }
        finally
        {
            HeavyTestGate.Semaphore.Release();
        }
    }
}
