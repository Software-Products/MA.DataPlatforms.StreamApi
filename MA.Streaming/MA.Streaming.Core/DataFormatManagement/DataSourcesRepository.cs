// <copyright file="DataSourcesRepository.cs" company="Motion Applied Ltd.">
//
// Copyright 2025 Motion Applied Ltd
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

using MA.DataPlatforms.Secu4.KafkaMetadataComponent;
using MA.Streaming.Abstraction;

namespace MA.Streaming.Core.DataFormatManagement;

public class DataSourcesRepository : IDataSourcesRepository
{
    private readonly IKafkaTopicHelper kafkaTopicHelper;
    private readonly IDomainKafkaRouteMappingInfoCreator domainKafkaRouteMappingInfoCreator;
    private readonly IStreamingApiConfiguration streamingApiConfiguration;

    private readonly object readWriteLock = new();
    private HashSet<string> trackedTopics = [];

    private bool initiated;

    public DataSourcesRepository(
        IKafkaTopicHelper kafkaTopicHelper,
        IStreamingApiConfigurationProvider streamingApiConfigurationProvider,
        IDomainKafkaRouteMappingInfoCreator domainKafkaRouteMappingInfoCreator)
    {
        this.kafkaTopicHelper = kafkaTopicHelper;
        this.domainKafkaRouteMappingInfoCreator = domainKafkaRouteMappingInfoCreator;
        this.streamingApiConfiguration = streamingApiConfigurationProvider.Provide();
    }

    public event EventHandler<string>? DataSourceAdded;

    public void Add(string dataSource)
    {
        lock (this.readWriteLock)
        {
            if (!this.initiated)
            {
                this.Initiate();
            }

            if (!this.trackedTopics.Add(dataSource))
            {
                return;
            }

            this.DataSourceAdded?.Invoke(this, dataSource);
        }
    }

    public IReadOnlyList<string> GetAll()
    {
        lock (this.readWriteLock)
        {
            return [.. this.trackedTopics];
        }
    }

    public void Initiate()
    {
        if (this.initiated)
        {
            return;
        }

        lock (this.readWriteLock)
        {
            var essentialsPrefix = this.domainKafkaRouteMappingInfoCreator.CreateEssentialTopicName("").Topic;
            var essentialTopics = this.kafkaTopicHelper.GetInfoByTopicPrefix(this.streamingApiConfiguration.BrokerUrl, essentialsPrefix);
            this.trackedTopics = essentialTopics
                .Select(i => i.TopicName.Replace(essentialsPrefix, string.Empty)).Distinct().ToHashSet();
            this.initiated = true;
        }
    }
}
