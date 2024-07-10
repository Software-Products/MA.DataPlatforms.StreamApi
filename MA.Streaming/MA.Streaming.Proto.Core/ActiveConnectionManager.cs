// <copyright file="ActiveConnectionManager.cs" company="McLaren Applied Ltd.">
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

using MA.Streaming.API;
using MA.Streaming.PrometheusMetrics;
using MA.Streaming.Proto.Core.Abstractions;

namespace MA.Streaming.Proto.Core;

public class ActiveConnectionManager : IActiveConnectionManager
{
    private readonly ConcurrentDictionary<long, ConnectionDetails> connections = new();
    private readonly ConcurrentDictionary<string, List<long>> dataSourceConnections = new();

    public event EventHandler<long>? ConnectionRemoved;

    public bool TryAddConnection(long id, ConnectionDetails details)
    {
        var result = this.connections.TryAdd(id, details);
        if (!result)
        {
            return result;
        }

        this.dataSourceConnections.TryGetValue(details.DataSource, out var foundDataSourceList);
        if (foundDataSourceList != null)
        {
            foundDataSourceList.Add(id);
        }
        else
        {
            this.dataSourceConnections.TryAdd(
                details.DataSource,
                new List<long>
                {
                    id
                });
        }

        this.SetMetric();

        return result;
    }

    private void SetMetric()
    {
        foreach (var dataSourceConnection in this.dataSourceConnections)
        {
            MetricProviders.NumberOfConnections.WithLabels(dataSourceConnection.Key).Set(dataSourceConnection.Value.Count);
        }
    }

    public bool TryGetConnection(long id, out ConnectionDetails? details)
    {
        return this.connections.TryGetValue(id, out details);
    }

    public bool TryRemoveConnection(long id)
    {
        var tryRemoveConnection = this.connections.TryRemove(id, out var removedItem);
        if (!tryRemoveConnection)
        {
            return tryRemoveConnection;
        }

        this.ConnectionRemoved?.Invoke(this, id);
        if (removedItem == null)
        {
            return tryRemoveConnection;
        }

        this.dataSourceConnections[removedItem.DataSource].RemoveAll(i => i == id);
        this.SetMetric();
        return tryRemoveConnection;
    }
}
