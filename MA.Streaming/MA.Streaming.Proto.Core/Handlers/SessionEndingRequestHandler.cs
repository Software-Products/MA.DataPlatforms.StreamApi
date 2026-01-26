// <copyright file="SessionEndingRequestHandler.cs" company="Motion Applied Ltd.">
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
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.SessionManagement;
using MA.Streaming.OpenData;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Handlers;

public class SessionEndingRequestHandler : ISessionEndingRequestHandler
{
    private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository;
    private readonly IPacketWriterHelper packetWriterHelper;
    private readonly ISessionInfoService sessionInfoService;
    private readonly IKafkaBrokerRoutesOffsetExtractor kafkaBrokerRoutesOffsetExtractor;
    private readonly ITypeNameProvider typeNameProvider;
    private readonly ISessionPacketWritingLogProvider sessionPacketLogProvider;
    private readonly IStreamStoppedPacketRequestFactory streamStoppedPacketRequestFactory;
    private readonly IStreamManager streamManager;

    public SessionEndingRequestHandler(
        IKafkaBrokerRoutesOffsetExtractor kafkaBrokerRoutesOffsetExtractor,
        ITypeNameProvider typeNameProvider,
        IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository,
        IPacketWriterHelper packetWriterHelper,
        ISessionInfoService sessionInfoService,
        ISessionPacketWritingLogProvider sessionPacketLogProvider,
        IStreamStoppedPacketRequestFactory streamStoppedPacketRequestFactory,
        IStreamManager streamManager)
    {
        this.sessionInfoRepository = sessionInfoRepository;
        this.packetWriterHelper = packetWriterHelper;
        this.sessionInfoService = sessionInfoService;
        this.kafkaBrokerRoutesOffsetExtractor = kafkaBrokerRoutesOffsetExtractor;
        this.sessionPacketLogProvider = sessionPacketLogProvider;
        this.streamStoppedPacketRequestFactory = streamStoppedPacketRequestFactory;
        this.streamManager = streamManager;
        this.typeNameProvider = typeNameProvider;
    }

    public EndSessionResponse Handle(EndSessionRequest request)
    {
        var sessionDetailRecord = this.sessionInfoRepository.Get(request.SessionKey);
        if (sessionDetailRecord is null)
        {
            return CreateUnsuccessfulResponse();
        }

        var partitionOffsetsInfos = this.kafkaBrokerRoutesOffsetExtractor.Extract(sessionDetailRecord.DataSource);

        var endOfSession = CreateEndOfSessionPacket(sessionDetailRecord, partitionOffsetsInfos, request.TerminationTime);

        var res = this.sessionInfoService.EndSession(request.SessionKey, partitionOffsetsInfos, endOfSession.TerminationTime.ToDateTime());
        if (!res.Success)
        {
            return CreateUnsuccessfulResponse();
        }

        this.WritePacket(request, endOfSession, sessionDetailRecord);

        return CreateSuccessfulResponse(endOfSession);
    }

    private void WritePacket(EndSessionRequest request, EndOfSessionPacket endOfSession, SessionDetailRecord sessionDetailRecord)
    {
        this.EndAllActiveStreams(request);

        this.packetWriterHelper.WriteInfoPacket(
            request.SessionKey,
            this.typeNameProvider.EndOfSessionPacketTypeName,
            endOfSession.ToByteString(),
            sessionDetailRecord.DataSource);
    }

    private static EndSessionResponse CreateSuccessfulResponse(EndOfSessionPacket endOfSession)
    {
        var endSessionResponse = new EndSessionResponse
        {
            EndSession = endOfSession,
            Success = true
        };
        return endSessionResponse;
    }

    private static EndSessionResponse CreateUnsuccessfulResponse()
    {
        var endSessionResponse = new EndSessionResponse
        {
            Success = false
        };
        return endSessionResponse;
    }

    private static EndOfSessionPacket CreateEndOfSessionPacket(
        SessionDetailRecord sessionDetailRecord,
        IEnumerable<TopicPartitionOffsetDto> topicPartitionOffsets,
        Timestamp? sessionTerminationTime)
    {
        var protoTopicOffsets = new MapField<string, long>();
        foreach (var topicPartitionOffset in topicPartitionOffsets)
        {
            protoTopicOffsets.Add(topicPartitionOffset.Title, topicPartitionOffset.Offset);
        }

        var endOfSession = new EndOfSessionPacket
        {
            DataSource = sessionDetailRecord.DataSource,
            TopicPartitionOffsets =
            {
                protoTopicOffsets
            },
            TerminationTime = sessionTerminationTime ?? Timestamp.FromDateTime(DateTime.UtcNow)
        };
        return endOfSession;
    }

    private void EndAllActiveStreams(EndSessionRequest request)
    {
        var sessionWritingLog = this.sessionPacketLogProvider.Provide(request.DataSource, request.SessionKey);
        var streamsToEnd = sessionWritingLog.GetActiveStreams();

        foreach (var stream in streamsToEnd.Where(i => i != ConstantSystemKeyStreamNames.EssentialStreamName)
                     .Where(stream => this.streamManager.CheckIfStreamIsEnded(request.DataSource, request.SessionKey, stream)))
        {
            this.packetWriterHelper.WriteDataPacket(
                this.streamStoppedPacketRequestFactory.CreateStreamStoppedPacketWritingRequest(
                    request.DataSource,
                    request.SessionKey,
                    stream,
                    false));
        }

        WaitForTheWritingToFinish(sessionWritingLog);
    }

    private static void WaitForTheWritingToFinish(ISessionPacketWritingLog sessionWritingLog)
    {
        Task.Delay(1000).Wait();
        while (DateTime.Now - sessionWritingLog.GetWhenLastUpdated() < TimeSpan.FromSeconds(1))
        {
            Task.Delay(1000).Wait();
        }
    }
}
