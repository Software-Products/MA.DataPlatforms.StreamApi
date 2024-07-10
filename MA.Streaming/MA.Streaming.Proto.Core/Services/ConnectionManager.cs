// <copyright file="ConnectionManager.cs" company="McLaren Applied Ltd.">
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

using Grpc.Core;

using MA.Streaming.API;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core.Services;

public sealed class ConnectionManager : ConnectionManagerService.ConnectionManagerServiceBase
{
    private readonly IActiveConnectionManager activeConnectionManager;

    public ConnectionManager(IActiveConnectionManager activeConnectionManager)
    {
        this.activeConnectionManager = activeConnectionManager;
    }

    public override Task<NewConnectionResponse> NewConnection(NewConnectionRequest request, ServerCallContext context)
    {
        var connectionHash = request.GetHashCode();
        var id = (long)connectionHash << 32 | (long)(uint)(new Random().Next());

        var connection = new Connection();

        if (this.activeConnectionManager.TryAddConnection(id, request.Details))
        {
            connection.Id = id;
        }

        return Task.FromResult(
            new NewConnectionResponse
            {
                Connection = connection
            });
    }

    public override Task<GetConnectionResponse> GetConnection(GetConnectionRequest request, ServerCallContext context)
    {
        var response = new GetConnectionResponse();

        if (this.activeConnectionManager.TryGetConnection(request.Connection.Id, out var value))
        {
            response.Details = value;
        }

        return Task.FromResult(response);
    }

    public override Task<CloseConnectionResponse> CloseConnection(CloseConnectionRequest request, ServerCallContext context)
    {
        var response = new CloseConnectionResponse
        {
            Success = this.activeConnectionManager.TryRemoveConnection(request.Connection.Id)
        };

        return Task.FromResult(response);
    }
}
