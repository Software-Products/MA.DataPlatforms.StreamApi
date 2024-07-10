// <copyright file="SessionManager.cs" company="McLaren Applied Ltd.">
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

using System.Collections.Concurrent;

using Google.Protobuf;
using Google.Protobuf.Collections;

using Grpc.Core;

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core.SessionManagement;
using MA.Streaming.OpenData;

namespace MA.Streaming.Proto.Core.Services;

public sealed class SessionManager : SessionManagementService.SessionManagementServiceBase
{
    private readonly IKeyGeneratorService keyGeneratorService;
    private readonly IRouteInfoProvider routeInfoProvider;
    private readonly IPacketWriterConnectorService packetWriterConnectorService;
    private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository;
    private readonly ILogger apiLogger;

    private readonly CancellationToken token;
    private readonly ConcurrentDictionary<string, List<IServerStreamWriter<GetSessionStartNotificationResponse>>> startNewSessionNotificationsWritersDic = new();
    private readonly ConcurrentDictionary<string, List<IServerStreamWriter<GetSessionStopNotificationResponse>>> stopSessionNotificationsWritersDic = new();
    private readonly bool enabled;

    public SessionManager(
        IKeyGeneratorService keyGeneratorService,
        IRouteInfoProvider routeInfoProvider,
        IPacketWriterConnectorService packetWriterConnectorService,
        ISessionInfoService sessionInfoService,
        IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository,
        ICancellationTokenSourceProvider cancellationTokenSourceProvider,
        ILogger apiLogger,
        IStreamingApiConfigurationProvider apiConfigurationProvider)
    {
        this.keyGeneratorService = keyGeneratorService;
        this.routeInfoProvider = routeInfoProvider;
        this.packetWriterConnectorService = packetWriterConnectorService;
        this.sessionInfoRepository = sessionInfoRepository;
        this.apiLogger = apiLogger;
        this.token = cancellationTokenSourceProvider.Provide().Token;
        sessionInfoService.SessionsInfoChanged += this.SessionInfoService_SessionsInfoChanged;
        this.enabled = apiConfigurationProvider.Provide().IntegrateSessionManagement;
    }

    public override async Task<CreateSessionResponse> CreateSession(CreateSessionRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new CreateSessionResponse();
        }

        var sessionKey = this.keyGeneratorService.GenerateStringKey();
        var topicPartitionOffsets = this.GetTopicPartitionOffsets(request.DataSource);

        var newSessionPacket = new NewSessionPacket
        {
            DataSource = request.DataSource,
            TopicPartitionOffsets =
            {
                topicPartitionOffsets
            }
        };
        this.WriteInfoPacket(sessionKey, nameof(NewSessionPacket), newSessionPacket.ToByteString(), request.DataSource);

        var sessionInfoPacket = new SessionInfoPacket
        {
            Type = request.Type,
            Version = request.Version,
            DataSource = request.DataSource
        };
        this.WriteInfoPacket(sessionKey, nameof(SessionInfoPacket), sessionInfoPacket.ToByteString(), request.DataSource);
        var response = new CreateSessionResponse
        {
            NewSession = newSessionPacket,
            SessionKey = sessionKey
        };

        return await Task.FromResult(response);
    }

    public override async Task<EndSessionResponse> EndSession(EndSessionRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new EndSessionResponse();
        }

        var sessionDetailRecord = this.sessionInfoRepository.Get(request.SessionKey);
        if (sessionDetailRecord is null)
        {
            return new EndSessionResponse();
        }

        var topicPartitionOffsets = this.GetTopicPartitionOffsets(sessionDetailRecord.DataSource);

        var endOfSession = new EndOfSessionPacket
        {
            DataSource = sessionDetailRecord.DataSource,
            TopicPartitionOffsets =
            {
                topicPartitionOffsets
            }
        };
        this.WriteInfoPacket(request.SessionKey, nameof(EndOfSessionPacket), endOfSession.ToByteString(), sessionDetailRecord.DataSource);

        return await Task.FromResult(
            new EndSessionResponse
            {
                EndSession = endOfSession
            });
    }

    public override async Task<GetCurrentSessionsResponse> GetCurrentSessions(GetCurrentSessionsRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new GetCurrentSessionsResponse();
        }

        var allSession = this.sessionInfoRepository.GetAll().Where(i => i.DataSource == request.DataSource);
        return await Task.FromResult(
            new GetCurrentSessionsResponse
            {
                SessionKeys =
                {
                    allSession.Select(i => i.SessionKey)
                }
            });
    }

    public override async Task<GetSessionInfoResponse> GetSessionInfo(GetSessionInfoRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new GetSessionInfoResponse();
        }

        var foundSessionDetail = this.sessionInfoRepository.Get(request.SessionKey);
        if (foundSessionDetail == null)
        {
            return new GetSessionInfoResponse();
        }

        return await Task.FromResult(
            new GetSessionInfoResponse
            {
                Type = foundSessionDetail.SessionInfoPacket.Type,
                DataSource = foundSessionDetail.DataSource,
                AssociateSessionKeys =
                {
                    foundSessionDetail.SessionInfoPacket.AssociatedKeys
                },
                Identifier = foundSessionDetail.SessionInfoPacket.Identifier,
                Version = foundSessionDetail.SessionInfoPacket.Version,
                IsComplete = foundSessionDetail.Completed
            });
    }

    public override async Task GetSessionStartNotification(
        GetSessionStartNotificationRequest request,
        IServerStreamWriter<GetSessionStartNotificationResponse> responseStream,
        ServerCallContext context)
    {
        if (!this.enabled)
        {
            return;
        }

        if (this.startNewSessionNotificationsWritersDic.TryGetValue(request.DataSource, out var lsServerStreamWriters))
        {
            lsServerStreamWriters.Add(responseStream);
        }
        else
        {
            this.startNewSessionNotificationsWritersDic.AddOrUpdate(
                request.DataSource,
                new List<IServerStreamWriter<GetSessionStartNotificationResponse>>
                {
                    responseStream
                },
                (_, o) =>
                {
                    o.Add(responseStream);
                    return o;
                }
            );
        }

        await Task.Run(() => this.token.WaitHandle.WaitOne(), this.token);
    }

    public override async Task GetSessionStopNotification(
        GetSessionStopNotificationRequest request,
        IServerStreamWriter<GetSessionStopNotificationResponse> responseStream,
        ServerCallContext context)
    {
        if (!this.enabled)
        {
            return;
        }

        if (this.stopSessionNotificationsWritersDic.TryGetValue(request.DataSource, out var lsServerStreamWriters))
        {
            lsServerStreamWriters.Add(responseStream);
        }
        else
        {
            this.stopSessionNotificationsWritersDic.AddOrUpdate(
                request.DataSource,
                new List<IServerStreamWriter<GetSessionStopNotificationResponse>>
                {
                    responseStream
                },
                (_, o) =>
                {
                    o.Add(responseStream);
                    return o;
                }
            );
        }

        await Task.Run(() => this.token.WaitHandle.WaitOne(), this.token);
    }

    public override async Task<UpdateSessionIdentifierResponse> UpdateSessionIdentifier(UpdateSessionIdentifierRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new UpdateSessionIdentifierResponse();
        }

        var foundSessionDetail = this.sessionInfoRepository.Get(request.SessionKey);
        if (foundSessionDetail == null)
        {
            this.apiLogger.Error($"try to update identifier for a session which is not added in the session management service. session key:{request.SessionKey}");
            return await Task.FromResult(
                new UpdateSessionIdentifierResponse
                {
                    Success = false
                });
        }

        var sessionInfo = new SessionInfoPacket
        {
            Type = foundSessionDetail.SessionInfoPacket.Type,
            AssociateSessionKeys =
            {
                foundSessionDetail.SessionInfoPacket.AssociatedKeys
            },
            DataSource = foundSessionDetail.DataSource,
            Identifier = request.Identifier,
            Version = foundSessionDetail.SessionInfoPacket.Version
        };

        this.WriteInfoPacket(request.SessionKey, nameof(SessionInfoPacket), sessionInfo.ToByteString(), foundSessionDetail.DataSource);

        return new UpdateSessionIdentifierResponse
        {
            Success = true
        };
    }

    public override async Task<AddAssociateSessionResponse> AddAssociateSession(AddAssociateSessionRequest request, ServerCallContext context)
    {
        if (!this.enabled)
        {
            return new AddAssociateSessionResponse();
        }

        var foundSessionDetail = this.sessionInfoRepository.Get(request.SessionKey);
        if (foundSessionDetail == null)
        {
            this.apiLogger.Error(
                $"try to add associatedId to an identifier for a session which is not added in the session management service. session key:{request.SessionKey}");
            return await Task.FromResult(
                new AddAssociateSessionResponse
                {
                    Success = false
                });
        }

        var sessionInfoAssociatedIds = new List<string>(foundSessionDetail.SessionInfoPacket.AssociatedKeys)
        {
            request.AssociateSessionKey
        };

        var sessionInfo = new SessionInfoPacket
        {
            Type = foundSessionDetail.SessionInfoPacket.Type,
            AssociateSessionKeys =
            {
                sessionInfoAssociatedIds
            },
            DataSource = foundSessionDetail.DataSource,
            Identifier = foundSessionDetail.SessionInfoPacket.Identifier,
            Version = foundSessionDetail.SessionInfoPacket.Version
        };

        this.WriteInfoPacket(request.SessionKey, nameof(SessionInfoPacket), sessionInfo.ToByteString(), foundSessionDetail.DataSource);

        return new AddAssociateSessionResponse
        {
            Success = true
        };
    }

    private void SessionInfoService_SessionsInfoChanged(object? sender, SessionsInfoChangeEventArg e)
    {
        switch (e.SessionActionType)
        {
            case SessionActionType.None:
            case SessionActionType.SessionDetailUpdated:
            default:
                return;
            case SessionActionType.NewSessionStarted:
                if (!this.startNewSessionNotificationsWritersDic.TryGetValue(e.DataSource, out var startNotificationWriters))
                {
                    return;
                }

                foreach (var serverStreamWriter in startNotificationWriters)
                {
                    serverStreamWriter.WriteAsync(
                        new GetSessionStartNotificationResponse
                        {
                            DataSource = e.DataSource,
                            SessionKey = e.SessionKey
                        },
                        this.token).Wait(this.token);
                }

                return;
            case SessionActionType.SessionCompleted:
                if (!this.stopSessionNotificationsWritersDic.TryGetValue(e.DataSource, out var stopNotificationWriters))
                {
                    return;
                }

                foreach (var serverStreamWriter in stopNotificationWriters)
                {
                    serverStreamWriter.WriteAsync(
                        new GetSessionStopNotificationResponse
                        {
                            DataSource = e.DataSource,
                            SessionKey = e.SessionKey
                        },
                        this.token).Wait(this.token);
                }

                return;
        }
    }

    private MapField<string, long> GetTopicPartitionOffsets(string dataSource)
    {
        var result = new MapField<string, long>();
        var routeInfos = this.routeInfoProvider.GetRouteInfo(dataSource).Cast<KafkaRouteInfo>().ToList();
        foreach (var routeInfo in routeInfos)
        {
            result.Add($"{routeInfo.Topic}:{routeInfo.Partition}", routeInfo.Offset);
        }

        return result;
    }

    private void WriteInfoPacket(string sessionKey, string packetType, ByteString content, string dataSource)
    {
        var packet = new Packet
        {
            Content = content,
            IsEssential = false,
            SessionKey = sessionKey,
            Type = packetType
        };

        this.packetWriterConnectorService.WriteInfoPacket(
            new WriteInfoPacketRequestDto(
                new PacketDto(packetType, sessionKey, false, content.ToByteArray()),
                dataSource,
                InfoTypeDto.SessionInfo,
                packet.ToByteArray()));
    }
}
