// <copyright file="ThreadSafeInMemoryRepository.cs" company="McLaren Applied Ltd.">
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

using MA.Streaming.Abstraction;

namespace MA.Streaming.Core;

public class ThreadSafeInMemoryRepository<TKey, TData> : IInMemoryRepository<TKey, TData>
    where TKey : notnull
{
    private readonly Dictionary<TKey, TData> dataStore = new();
    private readonly ReaderWriterLockSlim @lock = new();

    public void AddOrUpdate(TKey key, TData data)
    {
        this.@lock.EnterWriteLock();
        try
        {
            this.dataStore[key] = data;
        }
        finally
        {
            this.@lock.ExitWriteLock();
        }
    }

    public TData? Get(TKey key)
    {
        this.@lock.EnterReadLock();
        try
        {
            this.dataStore.TryGetValue(key, out var data);
            return data;
        }
        finally
        {
            this.@lock.ExitReadLock();
        }
    }

    public IReadOnlyList<TData> GetAll(Func<TData, bool>? predicate=null)
    {
        this.@lock.EnterReadLock();
        try
        {
            return predicate != null
                ? this.dataStore.Values.Where(predicate).ToList()
                : [.. this.dataStore.Values];
        }
        finally
        {
            this.@lock.ExitReadLock();
        }
    }

    public void Remove(TKey key)
    {
        this.@lock.EnterWriteLock();
        try
        {
            this.dataStore.Remove(key);
        }
        finally
        {
            this.@lock.ExitWriteLock();
        }
    }

    public void RemoveAll(Func<TData, bool>? predicate)
    {
        this.@lock.EnterWriteLock();
        try
        {
            if (predicate != null)
            {
                var keysToRemove = this.dataStore.Keys.Where(key => predicate(this.dataStore[key])).ToList();
                foreach (var key in keysToRemove)
                {
                    this.dataStore.Remove(key);
                }
            }
            else
            {
                this.dataStore.Clear();
            }
        }
        finally
        {
            this.@lock.ExitWriteLock();
        }
    }
}
