// <copyright file="DockerComposeCoordinator.cs" company="Motion Applied Ltd.">
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

using Docker.DotNet;
using Docker.DotNet.Models;

using MA.Streaming.IntegrationTests.Helper;

namespace MA.Streaming.IntegrationTests.Base;

/// <summary>
/// Reference-counted coordinator so multiple xUnit collection fixtures share one
/// docker-compose lifecycle: compose up on first Acquire, compose down on last Release.
/// </summary>
internal static class DockerComposeCoordinator
{
    private static int refCount;
    private static readonly SemaphoreSlim Gate = new SemaphoreSlim(1, 1);
    private static Task initTask = Task.CompletedTask;

    internal static async Task AcquireAsync()
    {
        await Gate.WaitAsync();
        Task toAwait;
        try
        {
            refCount++;
            if (refCount == 1)
            {
                initTask = StartDockerComposeAsync();
            }

            toAwait = initTask;
        }
        finally
        {
            Gate.Release();
        }

        await toAwait;
    }

    internal static async Task ReleaseAsync()
    {
        await Gate.WaitAsync();
        try
        {
            refCount--;
            if (refCount == 0)
            {
                await StopDockerComposeAsync();
            }
        }
        finally
        {
            Gate.Release();
        }
    }

    private static async Task StartDockerComposeAsync()
    {
        if (Environment.OSVersion.Platform != PlatformID.Win32NT)
        {
            await Task.Delay(TimeSpan.FromSeconds(10));
            return;
        }

        if (!DockerIsRunning())
        {
            throw new InvalidOperationException(
                "To run the tests, first start the Docker engine.");
        }

        var testDirectory = AppDomain.CurrentDomain.BaseDirectory;
        ShellCommandExecutor.Execute("net", "stop winnat");
        ShellCommandExecutor.Execute("net", "start winnat");
        ShellCommandExecutor.RunDockerCompose($"{testDirectory}\\docker-compose.yml", "stream-api");
        await WaitForContainerToStart("stream_api_integration_test_kafka");
        await WaitForContainerToStart("stream_api_integration_test_kafka_sasl");

        // Give both brokers time to fully initialise; SASL needs extra time to load the JAAS config
        await Task.Delay(TimeSpan.FromSeconds(20));
    }

    private static async Task StopDockerComposeAsync()
    {
        if (Environment.OSVersion.Platform != PlatformID.Win32NT)
        {
            await Task.Delay(TimeSpan.FromSeconds(5));
            return;
        }

        ShellCommandExecutor.Execute("docker compose", "-p stream-api down");
        ShellCommandExecutor.Execute("docker", "volume prune -f");

        // Give the Docker engine time to remove volumes and containers completely
        await Task.Delay(TimeSpan.FromSeconds(5));
    }

    private static async Task WaitForContainerToStart(string containerName)
    {
        const int MaxRetries = 60;
        const int RetryIntervalMilliseconds = 1000;
        for (var i = 0; i < MaxRetries; i++)
        {
            Thread.Sleep(RetryIntervalMilliseconds);
            try
            {
                using var client = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine")).CreateClient();
                var containers = await client.Containers.ListContainersAsync(
                    new ContainersListParameters { All = true });

                var container = containers.FirstOrDefault(c => c.Names[0].Contains(containerName));
                if (container != null)
                {
                    if (container.State == "running")
                    {
                        Console.WriteLine($"Container '{containerName}' is running.");
                        var hasStarted = await IsContainerStartedAsync(client, container.ID);
                        if (hasStarted)
                        {
                            Console.WriteLine($"Container '{containerName}' has started.");
                            return;
                        }

                        Console.WriteLine($"Container '{containerName}' has not started yet.");
                    }
                    else
                    {
                        Console.WriteLine($"Container '{containerName}' is not running.");
                    }
                }
                else
                {
                    Console.WriteLine($"Container '{containerName}' does not exist.");
                }
            }
            catch (Exception)
            {
                // Retry
            }
        }

        throw new TimeoutException(
            "Wait time for running docker-compose elapsed. " +
            "Check the docker-compose file and registry access.");
    }

    private static bool DockerIsRunning()
    {
        try
        {
            using var client = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine")).CreateClient();
            var info = client.System.GetSystemInfoAsync().Result;
            return info != null && !string.IsNullOrEmpty(info.ServerVersion);
        }
        catch
        {
            return false;
        }
    }

    private static async Task<bool> IsContainerStartedAsync(IDockerClient client, string containerId)
    {
        var containerInspect = await client.Containers.InspectContainerAsync(containerId);
        return containerInspect.State.Status == "running";
    }
}
