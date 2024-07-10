// <copyright file="MessageReceived.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.IntegrationTests.Helper;

public class MessageReceived<TKey, TData>
{
    public MessageReceived(string topic, TKey? key, TData? data, DateTime receiveTime)
    {
        this.Topic = topic;
        this.Key = key;
        this.Data = data;
        this.ReceiveTime = receiveTime;
    }

    public string Topic { get; }

    public TKey? Key { get; }

    public TData? Data { get; }

    public DateTime ReceiveTime { get; }
}
