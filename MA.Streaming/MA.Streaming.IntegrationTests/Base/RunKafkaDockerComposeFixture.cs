// <copyright file="RunKafkaDockerComposeFixture.cs" company="Motion Applied Ltd.">
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

//********************************************************************************************
// Important Note:                                                                           *
// In the pipeline the test will run on ubuntu and docker-compose will run                   *
// by the pipeline so there is no need to run the docker-compose using the codes below       *
// This fixture will be used for Windows and running tests on the developer's machine        *
// Just make sure that the visual studio is running with admin privilege                     *
//********************************************************************************************

using Xunit;

// Run all tests serially. The ThreadLeak tests measure process-global thread
// count, so no other test may run concurrently with them.
[assembly: CollectionBehavior(CollectionBehavior.CollectionPerAssembly, DisableTestParallelization = true)]

namespace MA.Streaming.IntegrationTests.Base;

public class RunKafkaDockerComposeFixture : IAsyncLifetime
{
    public Task InitializeAsync() => DockerComposeCoordinator.AcquireAsync();

    public Task DisposeAsync() => DockerComposeCoordinator.ReleaseAsync();
}
