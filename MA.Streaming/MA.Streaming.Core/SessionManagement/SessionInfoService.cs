// <copyright file="SessionInfoService.cs" company="McLaren Applied Ltd.">
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

using System.Timers;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.Core.Routing.EssentialsRouting;
using MA.Streaming.PrometheusMetrics;

using Timer = System.Timers.Timer;

namespace MA.Streaming.Core.SessionManagement;

public class SessionInfoService : ISessionInfoService
{
    private const string SessionInfoRouteName = "Session_Info_Route";
    private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository;
    private readonly ISessionRouteSubscriberFactory sessionRouteSubscriberFactory;
    private readonly ILogger logger;
    private readonly IDtoFromByteFactory<PacketDto> packetDtoFromByteFactory;
    private readonly IDtoFromByteFactory<NewSessionPacketDto> newSessionDtoFromByteFactory;
    private readonly IDtoFromByteFactory<EndOfSessionPacketDto> endOfSessionDtoFromByteFactory;
    private readonly IEssentialTopicNameCreator essentialTopicNameCreator;
    private readonly IDataSourcesRepository dataSourcesRepository;
    private readonly IDtoFromByteFactory<SessionInfoPacketDto> sessionInfoDtoFromByteFactory;

    private readonly string newSessionPacketTypeName;

    private readonly string endOfSessionPacketTypeName;
    private readonly string sessionInfoPacketTypeName;
    private readonly object startingLock = new();
    private readonly Timer serviceStartingCompletedDetectionTimer;
    private readonly AutoResetEvent startingAutoResetEvent = new(false);
    private readonly uint initialisationTimeoutSeconds;
    private IRouteSubscriber? sessionRouteSubscriber;
    private bool started;
    private DateTime lastPacketTimeReceived;

    public SessionInfoService(
        IStreamingApiConfigurationProvider apiConfigurationProvider,
        IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository,
        ISessionRouteSubscriberFactory sessionRouteSubscriberFactory,
        ILogger logger,
        IDtoFromByteFactory<PacketDto> packetDtoFromByteFactory,
        IDtoFromByteFactory<SessionInfoPacketDto> sessionInfoDtoFromByteFactory,
        IDtoFromByteFactory<NewSessionPacketDto> newSessionDtoFromByteFactory,
        IDtoFromByteFactory<EndOfSessionPacketDto> endOfSessionDtoFromByteFactory,
        ITypeNameProvider typeNameProvider,
        IEssentialTopicNameCreator essentialTopicNameCreator,
        IDataSourcesRepository dataSourcesRepository)
    {
        this.sessionInfoRepository = sessionInfoRepository;
        this.sessionRouteSubscriberFactory = sessionRouteSubscriberFactory;
        this.logger = logger;
        this.packetDtoFromByteFactory = packetDtoFromByteFactory;
        this.sessionInfoDtoFromByteFactory = sessionInfoDtoFromByteFactory;
        this.newSessionDtoFromByteFactory = newSessionDtoFromByteFactory;
        this.endOfSessionDtoFromByteFactory = endOfSessionDtoFromByteFactory;
        this.essentialTopicNameCreator = essentialTopicNameCreator;
        this.dataSourcesRepository = dataSourcesRepository;
        this.newSessionPacketTypeName = typeNameProvider.NewSessionPacketTypeName;
        this.endOfSessionPacketTypeName = typeNameProvider.EndOfSessionPacketTypeName;
        this.sessionInfoPacketTypeName = typeNameProvider.SessionInfoPacketTypeName;
        this.initialisationTimeoutSeconds = apiConfigurationProvider.Provide().InitialisationTimeoutSeconds > 1
            ? apiConfigurationProvider.Provide().InitialisationTimeoutSeconds
            : 3;
        this.serviceStartingCompletedDetectionTimer = new Timer(1000);
        this.serviceStartingCompletedDetectionTimer.Elapsed += this.ServiceStartingCompletedDetectionTimerElapsed;
    }

    public event EventHandler<SessionsInfoChangeEventArg>? SessionStarted;

    public event EventHandler<SessionsInfoChangeEventArg>? SessionStopped;

    public event EventHandler<SessionsInfoChangeEventArg>? SessionUpdated;

    public event EventHandler<DateTime>? ServiceStarted;

    public event EventHandler<DateTime>? ServiceStopped;

    public (bool Success, string Message) AddNewSession(string sessionKey, NewSessionPacketDto newSessionPacket)
    {
        try
        {
            var mainOffset = newSessionPacket.TopicOffsetsPartitions.FirstOrDefault(i => i.TopicName == newSessionPacket.DataSource && i.Partition == 0)?.Offset ?? 0;
            var essentialOffset = newSessionPacket.TopicOffsetsPartitions
                .FirstOrDefault(i => i.TopicName == this.essentialTopicNameCreator.Create(newSessionPacket.DataSource) && i.Partition == 0)?.Offset ?? 0;

            var sessionDetailRecord = new SessionDetailRecord(
                sessionKey,
                newSessionPacket.DataSource,
                newSessionPacket.TopicOffsetsPartitions,
                mainOffset,
                essentialOffset,
                newSessionPacket.OffsetFromUtc,
                0);
            if (newSessionPacket.SessionInfoPacketDto != null)
            {
                sessionDetailRecord.SetSessionInfo(newSessionPacket.SessionInfoPacketDto);
            }

            this.sessionInfoRepository.AddOrUpdate(
                sessionKey,
                sessionDetailRecord);

            this.dataSourcesRepository.Add(newSessionPacket.DataSource);          

            this.SessionStarted?.Invoke(this, new SessionsInfoChangeEventArg(sessionKey, newSessionPacket.DataSource));

            return (true, "ok");
        }
        catch (Exception ex)
        {
            return (false, $"exception happened during session creation: {ex}");
        }
    }

    public (bool Success, string Message) EndSession(string sessionKey, IReadOnlyList<TopicPartitionOffsetDto> partitionOffsetDto)
    {
        try
        {
            var foundItem = this.sessionInfoRepository.Get(sessionKey);
            if (foundItem is null)
            {
                this.logger.Error($"try to end the session that is not inserted before key:{sessionKey}");
                return (false, "the session with that key not found to end");
            }

            this.EndSession(foundItem, partitionOffsetDto);
            return (true, "ok");
        }
        catch (Exception ex)
        {
            return (false, $"exception happened during session ending: {ex}");
        }
    }

    public void Start()
    {
        lock (this.startingLock)
        {
            if (this.started)
            {
                return;
            }

            this.started = true;
            try
            {
                this.sessionRouteSubscriber = this.sessionRouteSubscriberFactory.Create(SessionInfoRouteName);
                this.sessionRouteSubscriber.PacketReceived += this.SessionRouteSubscriber_PacketReceived;
                this.sessionRouteSubscriber.Subscribe();
                this.lastPacketTimeReceived = DateTime.UtcNow;
                this.serviceStartingCompletedDetectionTimer.Enabled = true;
                this.startingAutoResetEvent.WaitOne();
                this.ServiceStarted?.Invoke(this, DateTime.UtcNow);
            }
            catch (Exception ex)
            {
                this.logger.Error(ex.ToString());
            }
        }
    }

    public void Stop()
    {
        if (this.sessionRouteSubscriber is null)
        {
            return;
        }

        this.sessionRouteSubscriber.Unsubscribe();
        this.sessionRouteSubscriber.PacketReceived -= this.SessionRouteSubscriber_PacketReceived;
        this.ServiceStopped?.Invoke(this, DateTime.UtcNow);
    }

    public (bool Success, string Message) UpdateSessionInfo(string sessionKey, SessionInfoPacketDto sessionInfo)
    {
        try
        {
            var foundItem = this.sessionInfoRepository.Get(sessionKey);
            if (foundItem is null)
            {
                this.logger.Error($"try to update the session that is not inserted before key:{sessionKey}");
                return (false, "the session with that key not found to update");
            }

            this.UpdateSessionInfo(foundItem, sessionInfo);
            return (true, "ok");
        }
        catch (Exception ex)
        {
            return (false, $"exception happened during session ending: {ex}");
        }
    }

    private void EndSession(SessionDetailRecord sessionDetailRecord, IReadOnlyList<TopicPartitionOffsetDto> partitionOffsetDto)
    {
        if (sessionDetailRecord.Completed)
        {
            return;
        }

        sessionDetailRecord.Complete(partitionOffsetDto);
        this.SessionStopped?.Invoke(
            this,
            new SessionsInfoChangeEventArg(sessionDetailRecord.SessionKey, sessionDetailRecord.DataSource));
    }

    private void UpdateSessionInfo(SessionDetailRecord foundItem, SessionInfoPacketDto sessionInfo)
    {
        var sessionInfoType = !string.IsNullOrEmpty(sessionInfo.Type) ? sessionInfo.Type : foundItem.SessionInfoPacket.Type;
        var sessionInfoVersion = sessionInfo.Version > 0 ? sessionInfo.Version : foundItem.SessionInfoPacket.Version;
        var sessionInfoIdentifier = !string.IsNullOrEmpty(sessionInfo.Identifier) ? sessionInfo.Identifier : foundItem.SessionInfoPacket.Identifier;
        var sessionInfoAssociatedIds = sessionInfo.AssociatedKeys.Any() ? sessionInfo.AssociatedKeys : foundItem.SessionInfoPacket.AssociatedKeys;
        var sessionInfoDetails = foundItem.SessionInfoPacket.Details.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        foreach (var newDetail in sessionInfo.Details)
        {
            sessionInfoDetails[newDetail.Key] = newDetail.Value;
        }

        var newSessionInfo = new SessionInfoPacketDto(
            sessionInfoType,
            sessionInfoVersion,
            sessionInfoIdentifier,
            sessionInfoAssociatedIds,
            sessionInfoDetails);

        if (foundItem.SessionInfoPacket.Equals(newSessionInfo))
        {
            return;
        }

        foundItem.SetSessionInfo(newSessionInfo);

        this.SessionUpdated?.Invoke(this, new SessionsInfoChangeEventArg(foundItem.SessionKey, foundItem.DataSource));
    }

    private void ServiceStartingCompletedDetectionTimerElapsed(object? sender, ElapsedEventArgs e)
    {
        this.serviceStartingCompletedDetectionTimer.Enabled = false;
        if ((DateTime.UtcNow - this.lastPacketTimeReceived).TotalSeconds < this.initialisationTimeoutSeconds)
        {
            this.serviceStartingCompletedDetectionTimer.Enabled = true;
            return;
        }

        this.startingAutoResetEvent.Set();
    }

    private void SessionRouteSubscriber_PacketReceived(object? sender, RoutingDataPacket e)
    {
        try
        {
            this.lastPacketTimeReceived = DateTime.UtcNow;
            var packet = this.GetPacket(e);
            if (packet is null)
            {
                return;
            }

            var foundItem = this.sessionInfoRepository.Get(packet.SessionKey);
            if (packet.Type == this.newSessionPacketTypeName)
            {
                if (foundItem != null)
                {
                    return;
                }

                this.HandleNewSessionPacketReceived(packet, packet.SessionKey);
                foreach (var dataSourcesSessions in this.sessionInfoRepository.GetAll().GroupBy(i => i.DataSource).ToList())
                {
                    var count = dataSourcesSessions.Count();
                    MetricProviders.NumberOfSessions.WithLabels(dataSourcesSessions.Key).Set(count);
                }
            }
            else
            {
                if (foundItem is null)
                {
                    this.logger.Error($"try to update or end the session that is not inserted before key:{packet.SessionKey}");
                    return;
                }

                if (packet.Type == this.endOfSessionPacketTypeName)
                {
                    this.HandleSessionEndPacketReceived(packet, foundItem);
                }

                else if (packet.Type == this.sessionInfoPacketTypeName)
                {
                    this.HandleSessionInfoUpdateRequestReceived(packet, foundItem);
                }
            }
        }
        catch (Exception exception)
        {
            this.logger.Error(
                $"error happened when reading session packet info from session topic. exception :{Environment.NewLine}{exception}");
        }
    }

    private PacketDto? GetPacket(RoutingDataPacket e)
    {
        if (e.Route != SessionInfoRouteName)
        {
            return null;
        }

        var sessionKey = e.Key;
        var packet = this.packetDtoFromByteFactory.ToDto(e.Message);
        if (packet is null ||
            sessionKey is null)
        {
            return null;
        }

        if (packet.Type != this.newSessionPacketTypeName &&
            packet.Type != this.sessionInfoPacketTypeName &&
            packet.Type != this.endOfSessionPacketTypeName)
        {
            this.logger.Warning($"the logic for type {packet.Type} is not implemented yet.");
        }

        return packet;
    }

    private void HandleSessionInfoUpdateRequestReceived(PacketDto packet, SessionDetailRecord foundItem)
    {
        var sessionInfo = this.sessionInfoDtoFromByteFactory.ToDto(packet.Content);
        if (sessionInfo is null)
        {
            return;
        }

        this.UpdateSessionInfo(foundItem, sessionInfo);
    }

    private void HandleSessionEndPacketReceived(PacketDto packet, SessionDetailRecord foundItem)
    {
        var endOfSession = this.endOfSessionDtoFromByteFactory.ToDto(packet.Content);
        if (endOfSession is null)
        {
            return;
        }

        this.EndSession(foundItem, endOfSession.TopicPartitionsOffset);
    }

    private void HandleNewSessionPacketReceived(PacketDto packet, string sessionKey)
    {
        var newSessionPacket = this.newSessionDtoFromByteFactory.ToDto(packet.Content);
        if (newSessionPacket is null)
        {
            return;
        }

        this.AddNewSession(sessionKey, newSessionPacket);
    }
}
