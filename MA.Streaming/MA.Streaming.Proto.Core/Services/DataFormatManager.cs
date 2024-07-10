// <copyright file="DataFormatManager.cs" company="McLaren Applied Ltd.">
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

using Google.Protobuf;

using Grpc.Core;

using MA.Common.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.API;
using MA.Streaming.Contracts;
using MA.Streaming.Core.DataFormatManagement;
using MA.Streaming.OpenData;

namespace MA.Streaming.Proto.Core.Services;

public sealed class DataFormatManager : DataFormatManagerService.DataFormatManagerServiceBase
{
    private readonly IKeyGeneratorService keyGeneratorService;
    private readonly IPacketWriterConnectorService packetWriterConnectorService;
    private readonly IInMemoryRepository<(string, string, DataFormatTypeDto), DataFormatRecord> dataFormatByParamIdentifierRepository;
    private readonly IInMemoryRepository<(string, ulong, DataFormatTypeDto), DataFormatRecord> dataFormatByUlongIdentifierRepository;
    private readonly IMapper<Packet, PacketDto> packetDtoMapper;
    private readonly IParameterListKeyIdentifierCreator parameterListKeyIdentifierCreator;

    public DataFormatManager(
        IKeyGeneratorService keyGeneratorService,
        IPacketWriterConnectorService packetWriterConnectorService,
        IInMemoryRepository<ValueTuple<string, string, DataFormatTypeDto>, DataFormatRecord> dataFormatByParamIdentifierRepository,
        IInMemoryRepository<ValueTuple<string, ulong, DataFormatTypeDto>, DataFormatRecord> dataFormatByUlongIdentifierRepository,
        IMapper<Packet, PacketDto> packetDtoMapper,
        IParameterListKeyIdentifierCreator parameterListKeyIdentifierCreator)
    {
        this.keyGeneratorService = keyGeneratorService;
        this.packetWriterConnectorService = packetWriterConnectorService;
        this.dataFormatByParamIdentifierRepository = dataFormatByParamIdentifierRepository;
        this.dataFormatByUlongIdentifierRepository = dataFormatByUlongIdentifierRepository;
        this.packetDtoMapper = packetDtoMapper;
        this.parameterListKeyIdentifierCreator = parameterListKeyIdentifierCreator;
    }

    public override async Task<GetEventDataFormatIdResponse> GetEventDataFormatId(GetEventDataFormatIdRequest request, ServerCallContext context)
    {
        var foundItem = this.dataFormatByParamIdentifierRepository.Get(
            new ValueTuple<string, string, DataFormatTypeDto>(request.DataSource, request.Event, DataFormatTypeDto.Event));
        if (foundItem != null)
        {
            return await Task.FromResult(
                new GetEventDataFormatIdResponse
                {
                    DataFormat = foundItem.Identifiers[0].ToString()
                });
        }

        var identifier = this.keyGeneratorService.GenerateUlongKey();
        var dataFormatDefinitionPacket = CreateEventDefinitionPacket(request.Event, identifier);
        var essentialPacket = CreateEssentialPacket(dataFormatDefinitionPacket);
        this.WriteDataPacket(essentialPacket, request.DataSource);
        return await Task.FromResult(
            new GetEventDataFormatIdResponse
            {
                DataFormat = identifier.ToString()
            });
    }

    private static Packet CreateEssentialPacket(DataFormatDefinitionPacket dataFormatDefinitionPacket)
    {
        var essentialPacket = new Packet
        {
            SessionKey = string.Empty,
            Content = dataFormatDefinitionPacket.ToByteString(),
            IsEssential = true,
            Type = nameof(DataFormatDefinitionPacket)
        };
        return essentialPacket;
    }

    private static DataFormatDefinitionPacket CreateEventDefinitionPacket(string eventIdentifier, ulong identifier)
    {
        var dataFormatDefinitionPacket = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Event,
            EventIdentifier = eventIdentifier,
            Identifier = identifier
        };
        return dataFormatDefinitionPacket;
    }

    private static DataFormatDefinitionPacket CreateParamListDefinitionPacket(IEnumerable<string> parametersList, ulong identifier)
    {
        var dataFormatDefinitionPacket = new DataFormatDefinitionPacket
        {
            Type = DataFormatType.Parameter,
            ParameterIdentifiers = new ParameterList
            {
                ParameterIdentifiers =
                {
                    parametersList
                }
            },
            Identifier = identifier
        };
        return dataFormatDefinitionPacket;
    }

    public override async Task<GetEventResponse> GetEvent(GetEventRequest request, ServerCallContext context)
    {
        if (!ulong.TryParse(request.DataFormat, out var ulongIdentifier))
        {
            return await Task.FromResult(new GetEventResponse());
        }

        var dataFormatRecord = this.dataFormatByUlongIdentifierRepository.Get(
            new ValueTuple<string, ulong, DataFormatTypeDto>(request.DataSource, ulongIdentifier, DataFormatTypeDto.Event));

        if (dataFormatRecord == null)
        {
            return await Task.FromResult(new GetEventResponse());
        }

        return await Task.FromResult(
            new GetEventResponse
            {
                Event = dataFormatRecord.ParametersIdentifiers[0]
            });
    }

    public override async Task<GetParameterDataFormatIdResponse> GetParameterDataFormatId(GetParameterDataFormatIdRequest request, ServerCallContext context)
    {
        var parametersIdentifierKey = this.parameterListKeyIdentifierCreator.Create(request.Parameters);
        var foundItem = this.dataFormatByParamIdentifierRepository.Get(
            new ValueTuple<string, string, DataFormatTypeDto>(request.DataSource, parametersIdentifierKey, DataFormatTypeDto.Parameter));

        if (foundItem != null)
        {
            var dataFormat = foundItem.Identifiers[0].ToString();
            return await Task.FromResult(
                new GetParameterDataFormatIdResponse
                {
                    DataFormat = dataFormat
                });
        }

        var identifier = this.keyGeneratorService.GenerateUlongKey();
        var dataFormatDefinitionPacket = CreateParamListDefinitionPacket(request.Parameters, identifier);
        var essentialPacket = CreateEssentialPacket(dataFormatDefinitionPacket);
        this.WriteDataPacket(essentialPacket, request.DataSource);
        return await Task.FromResult(
            new GetParameterDataFormatIdResponse
            {
                DataFormat = identifier.ToString()
            });
    }

    public override async Task<GetParametersListResponse> GetParametersList(GetParametersListRequest request, ServerCallContext context)
    {
        if (!ulong.TryParse(request.DataFormat, out var ulongIdentifier))
        {
            return await Task.FromResult(new GetParametersListResponse());
        }

        var dataFormatRecord = this.dataFormatByUlongIdentifierRepository.Get(
            new ValueTuple<string, ulong, DataFormatTypeDto>(request.DataSource, ulongIdentifier, DataFormatTypeDto.Parameter));
        if (dataFormatRecord == null)
        {
            return await Task.FromResult(new GetParametersListResponse());
        }

        return await Task.FromResult(
            new GetParametersListResponse
            {
                Parameters =
                {
                    dataFormatRecord.ParametersIdentifiers
                }
            });
    }

    private void WriteDataPacket(Packet packet, string dataSource)
    {
        this.packetWriterConnectorService.WriteDataPacket(
            new WriteDataPacketRequestDto(this.packetDtoMapper.Map(packet), dataSource, string.Empty, string.Empty, packet.ToByteArray()));
    }
}
