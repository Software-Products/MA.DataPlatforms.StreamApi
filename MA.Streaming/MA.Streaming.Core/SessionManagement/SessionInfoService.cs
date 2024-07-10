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

using MA.Common.Abstractions;
using MA.DataPlatform.Secu4.RouteSubscriberComponent.Abstractions;
using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.PrometheusMetrics;

namespace MA.Streaming.Core.SessionManagement
{
    public class SessionInfoService : ISessionInfoService
    {
        private const string SessionInfoRouteName = "Session_Info_Route";
        private readonly IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository;
        private readonly ISessionRouteSubscriberFactory sessionRouteSubscriberFactory;
        private readonly ILogger logger;
        private readonly IDtoFromByteFactory<PacketDto> packetDtoFromByteFactory;
        private readonly IDtoFromByteFactory<NewSessionPacketDto> newSessionDtoFromByteFactory;
        private readonly IDtoFromByteFactory<EndOfSessionPacketDto> endOfSessionDtoFromByteFactory;
        private readonly IDtoFromByteFactory<SessionInfoPacketDto> sessionInfoDtoFromByteFactory;

        private readonly string newSessionPacketTypeName;
        private readonly string endOfSessionPacketTypeName;
        private readonly string sessionInfoPacketTypeName;
        private readonly object startingLock = new();
        private IRouteSubscriber? sessionRouteSubscriber;
        private bool started;

        public SessionInfoService(
            IInMemoryRepository<string, SessionDetailRecord> sessionInfoRepository,
            ISessionRouteSubscriberFactory sessionRouteSubscriberFactory,
            ILogger logger,
            IDtoFromByteFactory<PacketDto> packetDtoFromByteFactory,
            IDtoFromByteFactory<SessionInfoPacketDto> sessionInfoDtoFromByteFactory,
            IDtoFromByteFactory<NewSessionPacketDto> newSessionDtoFromByteFactory,
            IDtoFromByteFactory<EndOfSessionPacketDto> endOfSessionDtoFromByteFactory,
            ITypeNameProvider typeNameProvider)
        {
            this.sessionInfoRepository = sessionInfoRepository;
            this.sessionRouteSubscriberFactory = sessionRouteSubscriberFactory;
            this.logger = logger;
            this.packetDtoFromByteFactory = packetDtoFromByteFactory;
            this.sessionInfoDtoFromByteFactory = sessionInfoDtoFromByteFactory;
            this.newSessionDtoFromByteFactory = newSessionDtoFromByteFactory;
            this.endOfSessionDtoFromByteFactory = endOfSessionDtoFromByteFactory;
            this.newSessionPacketTypeName = typeNameProvider.NewSessionPacketTypeName;
            this.endOfSessionPacketTypeName = typeNameProvider.EndOfSessionPacketTypeName;
            this.sessionInfoPacketTypeName = typeNameProvider.SessionInfoPacketTypeName;
        }

        public event EventHandler<SessionsInfoChangeEventArg>? SessionsInfoChanged;

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
        }

        private void SessionRouteSubscriber_PacketReceived(object? sender, RoutingDataPacket e)
        {
            try
            {
                var packet = this.GetPacket(e);
                if (packet is null)
                {
                    return;
                }

                if (packet.Type == this.newSessionPacketTypeName)
                {
                    this.HandleNewSessionPacketReceived(packet, packet.SessionKey);
                    foreach (var dataSourcesSessions in this.sessionInfoRepository.GetAll().GroupBy(i => i.DataSource).ToList())
                    {
                        var count = dataSourcesSessions.Count();
                        MetricProviders.NumberOfSessions.WithLabels(dataSourcesSessions.Key).Set(count);
                    }
                }
                else
                {
                    var foundItem = this.sessionInfoRepository.Get(packet.SessionKey);
                    if (foundItem is null)
                    {
                        this.logger.Error($"try to update or end the session that is not inserted before key:{packet.SessionKey}");
                        return;
                    }

                    if (packet.Type == this.endOfSessionPacketTypeName)
                    {
                        this.HandleSessionEndPacketReceived(packet, foundItem, packet.SessionKey);
                    }

                    else if (packet.Type == this.sessionInfoPacketTypeName)
                    {
                        this.HandleSessionInfoUpdateRequestReceived(packet, foundItem, packet.SessionKey);
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

        private void HandleSessionInfoUpdateRequestReceived(PacketDto packet, SessionDetailRecord foundItem, string sessionKey)
        {
            var sessionInfo = this.sessionInfoDtoFromByteFactory.ToDto(packet.Content);
            if (sessionInfo is null)
            {
                return;
            }

            var sessionInfoType = !string.IsNullOrEmpty(sessionInfo.Type) ? sessionInfo.Type : foundItem.SessionInfoPacket.Type;
            var sessionInfoVersion = sessionInfo.Version > 0 ? sessionInfo.Version : foundItem.SessionInfoPacket.Version;
            var sessionInfoIdentifier = !string.IsNullOrEmpty(sessionInfo.Identifier) ? sessionInfo.Identifier : foundItem.SessionInfoPacket.Identifier;
            var sessionInfoAssociatedIds = sessionInfo.AssociatedKeys.Any() ? sessionInfo.AssociatedKeys : foundItem.SessionInfoPacket.AssociatedKeys;
            foundItem.SetSessionInfo(new SessionInfoPacketDto(sessionInfoType, sessionInfoVersion, sessionInfoIdentifier, sessionInfoAssociatedIds));
            this.SessionsInfoChanged?.Invoke(this, new SessionsInfoChangeEventArg(sessionKey, foundItem.DataSource, SessionActionType.SessionDetailUpdated));
        }

        private void HandleSessionEndPacketReceived(PacketDto packet, SessionDetailRecord foundItem, string sessionKey)
        {
            var endOfSession = this.endOfSessionDtoFromByteFactory.ToDto(packet.Content);
            if (endOfSession is null)
            {
                return;
            }

            foundItem.Complete();
            this.SessionsInfoChanged?.Invoke(this, new SessionsInfoChangeEventArg(sessionKey, foundItem.DataSource, SessionActionType.SessionCompleted));
        }

        private void HandleNewSessionPacketReceived(PacketDto packet, string sessionKey)
        {
            var newSessionPacket = this.newSessionDtoFromByteFactory.ToDto(packet.Content);
            if (newSessionPacket is null)
            {
                return;
            }

            this.sessionInfoRepository.AddOrUpdate(
                sessionKey,
                new SessionDetailRecord(sessionKey, newSessionPacket.DataSource, newSessionPacket.TopicOffsetsPartitions));
            this.SessionsInfoChanged?.Invoke(this, new SessionsInfoChangeEventArg(sessionKey, newSessionPacket.DataSource, SessionActionType.NewSessionStarted));
        }
    }
}
