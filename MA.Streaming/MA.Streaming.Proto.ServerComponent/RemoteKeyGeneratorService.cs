// <copyright file="RemoteKeyGeneratorService.cs" company="McLaren Applied Ltd.">
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
using MA.KeyGenerator.Proto.Client;
using MA.Streaming.Abstraction;
using MA.Streaming.KeyGenerator;

namespace MA.Streaming.Proto.ServerComponent;

public class RemoteKeyGeneratorService : IKeyGeneratorService
{
    private readonly UniqueKeyGeneratorService.UniqueKeyGeneratorServiceClient client;

    public RemoteKeyGeneratorService(IStreamingApiConfigurationProvider streamingApiConfigurationProvider)
    {
        var config = streamingApiConfigurationProvider.Provide();
        KeyGeneratorClient.Initialise(config.RemoteKeyGeneratorServiceAddress);
        this.client = KeyGeneratorClient.GetKeyGeneratorClient();
    }

    public string GenerateStringKey()
    {
        return this.client.GenerateUniqueKey(
            new GenerateUniqueKeyRequest
            {
                Type = KeyType.String
            }).StringKey;
    }

    public ulong GenerateUlongKey()
    {
        return this.client.GenerateUniqueKey(
            new GenerateUniqueKeyRequest
            {
                Type = KeyType.Ulong
            }).UlongKey;
    }
}
