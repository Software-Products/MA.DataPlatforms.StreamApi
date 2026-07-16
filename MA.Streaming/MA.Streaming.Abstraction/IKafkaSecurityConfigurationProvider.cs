// <copyright file="IKafkaSecurityConfigurationProvider.cs" company="Motion Applied Ltd.">
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

namespace MA.Streaming.Abstraction;

/// <summary>
/// Supplies the Kafka security credentials used by the Stream API as the string-based
/// <see cref="KafkaSecurityConfiguration"/> POCO. Returns <c>null</c> when the deployment
/// is unsecured. Implementations are mapped internally onto the Secu4 routing library's
/// security configuration.
/// </summary>
public interface IKafkaSecurityConfigurationProvider
{
    KafkaSecurityConfiguration? Provide();
}
