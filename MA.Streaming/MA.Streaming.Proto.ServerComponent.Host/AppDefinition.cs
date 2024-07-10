// <copyright file="AppDefinition.cs" company="McLaren Applied Ltd.">
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

namespace MA.Streaming.Proto.ServerComponent.Host;

internal static class AppDefinition
{
    private static string listenerPortNumber = "";
    private static string prometheusMetricPort = "";
    private static string streamApiConfigFilePath = "";
    private static bool autoStart = false;

    private static bool isLoaded;

    public static string ListenerListenerPortNumber
    {
        get
        {
            if (!isLoaded)
            {
                Load();
            }

            return listenerPortNumber;
        }
    }

    public static string PrometheusMetricPort
    {
        get
        {
            if (!isLoaded)
            {
                Load();
            }

            return prometheusMetricPort;
        }
    }

    public static string StreamApiConfigFilePath
    {
        get
        {
            if (!isLoaded)
            {
                Load();
            }

            return streamApiConfigFilePath;
        }
    }

    public static bool AutoStart
    {
        get
        {
            if (!isLoaded)
            {
                Load();
            }

            return autoStart;
        }
    }

    public static void Load()
    {
        listenerPortNumber = Environment.GetEnvironmentVariable("LISTENER_PORT_NUMBER") ?? "1531";
        prometheusMetricPort = Environment.GetEnvironmentVariable("PROMETHEUS_METRIC_PORT") ?? "2468";
        streamApiConfigFilePath = Environment.GetEnvironmentVariable("CONFIG_PATH") ?? "/Configs/AppConfig.json";
        autoStart = bool.Parse(Environment.GetEnvironmentVariable("AUTO_START") ?? "true");
        isLoaded = true;
    }
}
