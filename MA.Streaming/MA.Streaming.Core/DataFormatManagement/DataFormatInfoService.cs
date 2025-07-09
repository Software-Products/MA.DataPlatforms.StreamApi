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

using System.Timers;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.Core.Abstractions;
using MA.Streaming.PrometheusMetrics;

using Timer = System.Timers.Timer;

namespace MA.Streaming.Core.DataFormatManagement;

public class DataFormatInfoService : IDataFormatInfoService
{
    private readonly IDataFormatRoutesFactory dataFormatRoutesFactory;
    private readonly IDataSourcesRepository dataSourcesRepository;
    private readonly IDataFormatRouteSubscriberFactory dataFormatRouteSubscriberFactory;
    private readonly IInMemoryRepository<string, List<DataFormatRecord>> dataSourceDataFormatsRepository;
    private readonly IInMemoryRepository<(string, string, DataFormatTypeDto), DataFormatRecord> stringToDataRecordRepository;
    private readonly IInMemoryRepository<(string, ulong, DataFormatTypeDto), DataFormatRecord> ulongIdToDataRecordRepository;
    private readonly IDtoFromByteFactory<PacketDto> packetDtoFromByteFactory;
    private readonly IDtoFromByteFactory<DataFormatDefinitionPacketDto> dataFormatDefinitionPacketFromByteFactory;
    private readonly IParameterListKeyIdentifierCreator parameterListKeyIdentifierCreator;
    private readonly ILogger logger;

    private readonly object startingLock = new();
    private readonly string dataFormatDefinitionPacketTypeName;
    private readonly List<IRouteSubscriber> dataFormatRouteSubscribers = [];
    private readonly Timer serviceStartingCompletedDetectionTimer;
    private readonly AutoResetEvent startingAutoResetEvent = new(false);
    private readonly uint initialisationTimeoutSeconds;
    private bool started;
    private DateTime lastPacketTimeReceived;

    public DataFormatInfoService(
        IStreamingApiConfigurationProvider apiConfigurationProvider,
        IDataFormatRoutesFactory dataFormatRoutesFactory,
        IDataSourcesRepository dataSourcesRepository,
        IDataFormatRouteSubscriberFactory dataFormatRouteSubscriberFactory,
        IInMemoryRepository<(string, string, DataFormatTypeDto), DataFormatRecord> stringToDataRecordRepository,
        IInMemoryRepository<(string, ulong, DataFormatTypeDto), DataFormatRecord> ulongIdToDataRecordRepository,
        IInMemoryRepository<string, List<DataFormatRecord>> dataSourceDataFormatsRepository,
        IDtoFromByteFactory<PacketDto> packetDtoFromByteFactory,
        IDtoFromByteFactory<DataFormatDefinitionPacketDto> dataFormatDefinitionPacketFromByteFactory,
        ITypeNameProvider typeNameProvider,
        IParameterListKeyIdentifierCreator parameterListKeyIdentifierCreator,
        ILogger logger)
    {
        this.dataFormatRoutesFactory = dataFormatRoutesFactory;
        this.dataSourcesRepository = dataSourcesRepository;
        this.dataFormatRouteSubscriberFactory = dataFormatRouteSubscriberFactory;
        this.stringToDataRecordRepository = stringToDataRecordRepository;
        this.ulongIdToDataRecordRepository = ulongIdToDataRecordRepository;
        this.packetDtoFromByteFactory = packetDtoFromByteFactory;
        this.dataFormatDefinitionPacketFromByteFactory = dataFormatDefinitionPacketFromByteFactory;
        this.parameterListKeyIdentifierCreator = parameterListKeyIdentifierCreator;
        this.logger = logger;
        this.dataSourceDataFormatsRepository = dataSourceDataFormatsRepository;
        this.dataFormatDefinitionPacketTypeName = typeNameProvider.DataFormatDefinitionPacketTypeName;
        this.initialisationTimeoutSeconds = apiConfigurationProvider.Provide().InitialisationTimeoutSeconds > 1
            ? apiConfigurationProvider.Provide().InitialisationTimeoutSeconds
            : 3;
        this.serviceStartingCompletedDetectionTimer = new Timer(1000);
        this.serviceStartingCompletedDetectionTimer.Elapsed += this.ServiceStartingCompletedDetectionTimerElapsed;
    }

    public event EventHandler<DataFormatRecord>? NewRecordFound;

    public event EventHandler<DateTime>? ServiceStarted;

    public event EventHandler<DateTime>? ServiceStopped;

    public IReadOnlyList<IRouteSubscriber> RouteSubscribers => this.dataFormatRouteSubscribers;

    public void Add(string dataSource, IReadOnlyList<string> identifiers, ulong dataFormatId, DataFormatTypeDto formatTypeDto)
    {
        var stringIdentifiersKey = this.parameterListKeyIdentifierCreator.Create(identifiers);

        this.AddDataFormatRecordToRepositories(dataSource, stringIdentifiersKey, identifiers, dataFormatId, formatTypeDto);
    }

    public DataFormatRecord? GetByDataFormatId(string dataSource, ulong dataFormatId, DataFormatTypeDto formatTypeDto)
    {
        return this.ulongIdToDataRecordRepository.Get((dataSource, dataFormatId, formatTypeDto));
    }

    public DataFormatRecord? GetByIdentifier(string dataSource, IReadOnlyList<string> identifiers, DataFormatTypeDto formatTypeDto)
    {
        var stringIdentifiersKey = this.parameterListKeyIdentifierCreator.Create(identifiers);
        return this.stringToDataRecordRepository.Get((dataSource, stringIdentifiersKey, formatTypeDto));
    }

    public IReadOnlyList<DataFormatRecord> GetDataSourceDataFormats(string datasource)
    {
        return this.dataSourceDataFormatsRepository.Get(datasource) ?? [];
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
                this.dataSourcesRepository.Initiate();
                var preExistDataSources = this.dataSourcesRepository.GetAll();
                foreach (var preExistDataSource in preExistDataSources)
                {
                    this.dataSourceDataFormatsRepository.AddOrUpdate(preExistDataSource, []);
                }

                this.dataSourcesRepository.DataSourceAdded += this.DataSourcesRepository_DataSourceAdded;
                this.CreateRouteSubscriberAndStartsSubscribing(preExistDataSources);
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
        this.dataFormatRouteSubscribers.ForEach(i =>
        {
            i.Unsubscribe();
            i.PacketReceived -= this.DataFormatRouteSubscriberPacketReceived;
        });

        this.ServiceStopped?.Invoke(this, DateTime.UtcNow);
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
        this.lastPacketTimeReceived = DateTime.UtcNow;
        this.serviceStartingCompletedDetectionTimer.Enabled = true;
        this.startingAutoResetEvent.WaitOne();
    }

    private void DataSourcesRepository_DataSourceAdded(object? sender, string e)
    {
        if (string.IsNullOrEmpty(e))
        {
            return;
        }

        this.dataSourceDataFormatsRepository.AddOrUpdate(e, []);
        this.CreateRouteSubscriberAndStartsSubscribing([e]);
    }

    private void DataFormatRouteSubscriberPacketReceived(object? sender, RoutingDataPacket e)
    {
        try
        {
            this.lastPacketTimeReceived = DateTime.UtcNow;
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
        return packet;
    }

    private void HandleDataFormatDefinitionPacketReceived(PacketDto packet, string dataSource)
    {
        var dataFormatDefinitionPacketDto = this.dataFormatDefinitionPacketFromByteFactory.ToDto(packet.Content);
        if (dataFormatDefinitionPacketDto is null)
        {
            return;
        }

        var dataFormatId = dataFormatDefinitionPacketDto.Identifier;
        var stringIdentifierKey = dataFormatDefinitionPacketDto.ContentIdentifiersKey;
        var stringIdentifiers = dataFormatDefinitionPacketDto.ContentIdentifiers;
        var dataFormatTypeDto = dataFormatDefinitionPacketDto.DataFormatTypeDto;

        this.AddDataFormatRecordToRepositories(dataSource, stringIdentifierKey, stringIdentifiers, dataFormatId, dataFormatTypeDto);
    }

    private void AddDataFormatRecordToRepositories(
        string dataSource,
        string stringIdentifierKey,
        IReadOnlyList<string> stringIdentifiers,
        ulong dataFormatId,
        DataFormatTypeDto dataFormatTypeDto)
    {
        var stringValueTupleKey = (
            dataSource,
            stringIdentifierKey,
            dataFormatTypeDto);

        var dataFormatRecord = this.stringToDataRecordRepository.Get(stringValueTupleKey);
        var newRecord = false;

        if (dataFormatRecord != null)
        {
            dataFormatRecord.AddIdentifier(dataFormatId);
        }
        else
        {
            dataFormatRecord = new DataFormatRecord(
                dataSource,
                stringIdentifierKey,
                stringIdentifiers,
                dataFormatTypeDto,
                dataFormatId);
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

            newRecord = true;
        }

        var ulongValueTupleKey = (
            dataSource,
            dataFormatId,
            dataFormatTypeDto);

        var ulongFoundItem = this.ulongIdToDataRecordRepository.Get(ulongValueTupleKey);

        if (ulongFoundItem == null)
        {
            this.ulongIdToDataRecordRepository.AddOrUpdate(ulongValueTupleKey, dataFormatRecord);
            newRecord = true;
        }

        if (newRecord)
        {
            this.NewRecordFound?.Invoke(this, dataFormatRecord);
        }
    }
}
