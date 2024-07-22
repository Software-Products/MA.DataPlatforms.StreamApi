// <copyright file="DataFormatDefinitionPacketDtoFromByteFactory.cs" company="McLaren Applied Ltd.">
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
using MA.Streaming.Abstraction;
using MA.Streaming.Contracts;
using MA.Streaming.OpenData;

namespace MA.Streaming.Proto.Core.Factories;

public class DataFormatDefinitionPacketDtoFromByteFactory : IDtoFromByteFactory<DataFormatDefinitionPacketDto>
{
    private readonly ILogger apiLogger;
    private readonly IParameterListKeyIdentifierCreator parameterListKeyIdentifierCreator;

    public DataFormatDefinitionPacketDtoFromByteFactory(ILogger apiLogger, IParameterListKeyIdentifierCreator parameterListKeyIdentifierCreator)
    {
        this.apiLogger = apiLogger;
        this.parameterListKeyIdentifierCreator = parameterListKeyIdentifierCreator;
    }

    public DataFormatDefinitionPacketDto? ToDto(byte[] content)
    {
        try
        {
            var packet = DataFormatDefinitionPacket.Parser.ParseFrom(content);
            var lstContentIdentifiers = new List<string>();
            if (packet.HasEventIdentifier)
            {
                lstContentIdentifiers.Add(packet.EventIdentifier);
            }

            if (packet.ParameterIdentifiers != null &&
                packet.ParameterIdentifiers.ParameterIdentifiers.Count > 0)
            {
                lstContentIdentifiers.AddRange(packet.ParameterIdentifiers.ParameterIdentifiers);
            }

            return new DataFormatDefinitionPacketDto(
                packet.Identifier,
                (DataFormatTypeDto)packet.Type,
                lstContentIdentifiers,
                this.parameterListKeyIdentifierCreator.Create(lstContentIdentifiers));
        }
        catch (Exception ex)
        {
            this.apiLogger.Error($"can not parsing the byte array content to the Packet. exception: {ex}");

            return null;
        }
    }
}
