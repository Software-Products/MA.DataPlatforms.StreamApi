// <copyright file="DataFormatInfoService.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Core.DataFormatManagement
{
    public class DataFormatInfoService : IDataFormatInfoService
    {
        private readonly IDataFormatRoutesFactory dataFormatRoutesFactory;
        private readonly IDataSourcesRepository dataSourcesRepository;
        private readonly IDataFormatRouteSubscriberFactory dataFormatRouteSubscriberFactory;
        private readonly IInMemoryRepository<string, List<DataFormatRecord>> dataSourceDataFormatsRepository;
        private readonly IInMemoryRepository<ValueTuple<string, string, DataFormatTypeDto>, DataFormatRecord> stringToDataRecordRepository;
        private readonly IInMemoryRepository<ValueTuple<string, ulong, DataFormatTypeDto>, DataFormatRecord> ulongIdToDataRecordRepository;
        private readonly IDtoFromByteFactory<PacketDto> packetDtoFromByteFactory;
        private readonly IDtoFromByteFactory<DataFormatDefinitionPacketDto> dataFormatDefinitionPacketFromByteFactory;
        private readonly ILogger logger;

        private readonly object startingLock = new();
        private readonly string dataFormatDefinitionPacketTypeName;
        private readonly List<IRouteSubscriber> dataFormatRouteSubscribers = [];
        private bool started;

        public DataFormatInfoService(
            IDataFormatRoutesFactory dataFormatRoutesFactory,
            IDataSourcesRepository dataSourcesRepository,
            IDataFormatRouteSubscriberFactory dataFormatRouteSubscriberFactory,
            IInMemoryRepository<ValueTuple<string, string, DataFormatTypeDto>, DataFormatRecord> stringToDataRecordRepository,
            IInMemoryRepository<ValueTuple<string, ulong, DataFormatTypeDto>, DataFormatRecord> ulongIdToDataRecordRepository,
            IInMemoryRepository<string, List<DataFormatRecord>> dataSourceDataFormatsRepository,
            IDtoFromByteFactory<PacketDto> packetDtoFromByteFactory,
            IDtoFromByteFactory<DataFormatDefinitionPacketDto> dataFormatDefinitionPacketFromByteFactory,
            ITypeNameProvider typeNameProvider,
            ILogger logger)
        {
            this.dataFormatRoutesFactory = dataFormatRoutesFactory;
            this.dataSourcesRepository = dataSourcesRepository;
            this.dataFormatRouteSubscriberFactory = dataFormatRouteSubscriberFactory;
            this.stringToDataRecordRepository = stringToDataRecordRepository;
            this.ulongIdToDataRecordRepository = ulongIdToDataRecordRepository;
            this.packetDtoFromByteFactory = packetDtoFromByteFactory;
            this.dataFormatDefinitionPacketFromByteFactory = dataFormatDefinitionPacketFromByteFactory;
            this.logger = logger;
            this.dataSourceDataFormatsRepository = dataSourceDataFormatsRepository;
            this.dataFormatDefinitionPacketTypeName = typeNameProvider.DataFormatDefinitionPacketTypeName;
        }

        public IReadOnlyList<IRouteSubscriber> RouteSubscribers => this.dataFormatRouteSubscribers;

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
                    this.dataSourcesRepository.Initiate();
                    var preExistDataSources = this.dataSourcesRepository.GetAll();
                    foreach (var preExistDataSource in preExistDataSources)
                    {
                        this.dataSourceDataFormatsRepository.AddOrUpdate(preExistDataSource, []);
                    }

                    this.CreateRouteSubscriberAndStartsSubscribing(preExistDataSources);
                    this.dataSourcesRepository.NewTRackingDataSourceAdded += this.DataSourcesRepository_NewTRackingDataSourceAdded;
                }
                catch (Exception ex)
                {
                    this.logger.Error(ex.ToString());
                }
            }
        }

        private void CreateRouteSubscriberAndStartsSubscribing(IReadOnlyList<string> dataSourcesToSubscribe)
        {
            if (dataSourcesToSubscribe.Count < 1)
            {
                return;
            }

            var routeSubscriber = this.dataFormatRouteSubscriberFactory.Create(this.dataFormatRoutesFactory.Create(dataSourcesToSubscribe));
            this.dataFormatRouteSubscribers.Add(routeSubscriber);
            routeSubscriber.PacketReceived += this.DataFormatRouteSubscriberPacketReceived;
            routeSubscriber.Subscribe();
        }

        private void DataSourcesRepository_NewTRackingDataSourceAdded(object? sender, string e)
        {
            if (string.IsNullOrEmpty(e))
            {
                return;
            }

            this.dataSourceDataFormatsRepository.AddOrUpdate(e, []);
            this.CreateRouteSubscriberAndStartsSubscribing([e]);
        }

        public void Stop()
        {
            this.dataFormatRouteSubscribers.ForEach(i => i.Unsubscribe());
        }

        private void DataFormatRouteSubscriberPacketReceived(object? sender, RoutingDataPacket e)
        {
            try
            {
                var packet = this.GetPacket(e);
                if (packet is null)
                {
                    return;
                }

                if (packet.Type != this.dataFormatDefinitionPacketTypeName)
                {
                    return;
                }

                this.HandleDataFormatDefinitionPacketReceived(packet, e.Route);
                foreach (var dataSource in this.dataSourcesRepository.GetAll())
                {
                    var foundRecords = this.dataSourceDataFormatsRepository.Get(dataSource);
                    MetricProviders.NumberOfDataFormats.WithLabels(dataSource).Set(foundRecords?.Count ?? 0);
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
            var packet = this.packetDtoFromByteFactory.ToDto(e.Message);
            if (packet is null)
            {
                return null;
            }

            if (packet.Type != this.dataFormatDefinitionPacketTypeName)
            {
                this.logger.Warning($"the logic for type {packet.Type} is not implemented yet.");
            }

            return packet;
        }

        private void HandleDataFormatDefinitionPacketReceived(PacketDto packet, string dataSource)
        {
            var dataFormatDefinitionPacketDto = this.dataFormatDefinitionPacketFromByteFactory.ToDto(packet.Content);
            if (dataFormatDefinitionPacketDto is null)
            {
                return;
            }

            var stringValueTupleKey = new ValueTuple<string, string, DataFormatTypeDto>(
                dataSource,
                dataFormatDefinitionPacketDto.ContentIdentifiersKey,
                dataFormatDefinitionPacketDto.DataFormatTypeDto);

            var stringKeyFoundItem = this.stringToDataRecordRepository.Get(stringValueTupleKey);

            var dataFormatRecord = new DataFormatRecord(
                dataSource,
                dataFormatDefinitionPacketDto.ContentIdentifiersKey,
                dataFormatDefinitionPacketDto.ContentIdentifiers,
                dataFormatDefinitionPacketDto.DataFormatTypeDto,
                dataFormatDefinitionPacketDto.Identifier);

            if (stringKeyFoundItem != null)
            {
                stringKeyFoundItem.AddIdentifier(dataFormatDefinitionPacketDto.Identifier);
            }
            else
            {
                this.stringToDataRecordRepository.AddOrUpdate(stringValueTupleKey, dataFormatRecord);
                var lstDataFormats = this.dataSourceDataFormatsRepository.Get(dataSource);
                if (lstDataFormats == null)
                {
                    this.dataSourceDataFormatsRepository.AddOrUpdate(dataSource, [dataFormatRecord]);
                }
                else
                {
                    lstDataFormats.Add(dataFormatRecord);
                }
            }

            var ulongValueTupleKey = new ValueTuple<string, ulong, DataFormatTypeDto>(
                dataSource,
                dataFormatDefinitionPacketDto.Identifier,
                dataFormatDefinitionPacketDto.DataFormatTypeDto);

            var ulongFoundItem = this.ulongIdToDataRecordRepository.Get(ulongValueTupleKey);

            if (ulongFoundItem == null)
            {
                this.ulongIdToDataRecordRepository.AddOrUpdate(ulongValueTupleKey, dataFormatRecord);
            }
        }
    }
}
