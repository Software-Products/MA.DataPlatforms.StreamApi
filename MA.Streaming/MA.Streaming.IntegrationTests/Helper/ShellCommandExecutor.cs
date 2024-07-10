// <copyright file="ShellCommandExecutor.cs" company="McLaren Applied Ltd.">
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

using System.Diagnostics;

namespace MA.Streaming.IntegrationTests.Helper
{
    public class ShellCommandExecutor
    {
        public static void Execute(string command, string arguments)
        {
            if (Environment.OSVersion.Platform != PlatformID.Win32NT)
            {
                return;
            }

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "powershell.exe",
                    Arguments = $"{command} {arguments}",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                }
            };
            process.Start();
            process.StandardOutput.ReadToEnd();
            process.WaitForExit();
        }

        public static void RunDockerCompose(string dockerComposeFilePath, string composeProjectName)
        {
            if (Environment.OSVersion.Platform != PlatformID.Win32NT)
            {
                return;
            }

            var psi = new ProcessStartInfo
            {
                FileName = (Environment.OSVersion.Platform == PlatformID.Win32NT) ? "docker-compose.exe" : "docker-compose",
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
                Arguments = $"-f \"{ConvertToUnixPath(dockerComposeFilePath)}\" -p {composeProjectName} up -d"
            };

            var process = new Process
            {
                StartInfo = psi,
                EnableRaisingEvents = true,
            };

            process.OutputDataReceived += (_, e) =>
            {
                if (!string.IsNullOrWhiteSpace(e.Data))
                {
                    Console.WriteLine(e.Data);
                }
            };

            process.ErrorDataReceived += (_, e) =>
            {
                if (!string.IsNullOrWhiteSpace(e.Data))
                {
                    Console.WriteLine("data received as error: " + e.Data);
                }
            };

            process.Start();
            Console.WriteLine("Docker Compose command executed.");
        }

        private static string ConvertToUnixPath(string windowsPath)
        {
            var normalizedPath = Path.Combine(windowsPath.Split('\\'));
            var unixPath = normalizedPath.Replace("\\", "/");
            return unixPath;
        }
    }
}
