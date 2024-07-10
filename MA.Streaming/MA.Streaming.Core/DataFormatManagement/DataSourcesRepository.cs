// <copyright file="DataSourcesRepository.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatform.Secu4.KafkaMetadataComponent;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Routing;

namespace MA.Streaming.Core.DataFormatManagement;

public class DataSourcesRepository : IDataSourcesRepository
{
    private readonly IKafkaTopicHelper kafkaTopicHelper;
    private readonly IStreamingApiConfiguration streamingApiConfiguration;
    private HashSet<string> trackedTopics = [];
    private readonly object lockObject = new();
    private bool initiated = false;

    public DataSourcesRepository(IKafkaTopicHelper kafkaTopicHelper, IStreamingApiConfigurationProvider streamingApiConfigurationProvider)
    {
        this.kafkaTopicHelper = kafkaTopicHelper;
        this.streamingApiConfiguration = streamingApiConfigurationProvider.Provide();
    }

    public event EventHandler<string>? NewTRackingDataSourceAdded;

    public void Initiate()
    {
        if (this.initiated)
        {
            return;
        }

        var essentialTopics = this.kafkaTopicHelper.GetInfoByTopicSuffix(
            this.streamingApiConfiguration.BrokerUrl,
            Constants.EssentialTopicNameSuffix);
        this.trackedTopics = essentialTopics
            .Select(i => i.TopicName.Replace(Constants.EssentialTopicNameSuffix, string.Empty)).Distinct().ToHashSet();
        this.initiated = true;
    }

    public IReadOnlyList<string> GetAll()
    {
        return [.. this.trackedTopics];
    }

    public void Add(string dataSource)
    {
        lock (this.lockObject)
        {
            if (!this.initiated)
            {
                this.Initiate();
            }

            if (!this.trackedTopics.Add(dataSource))
            {
                return;
            }

            this.NewTRackingDataSourceAdded?.Invoke(this, dataSource);
        }
    }
}
