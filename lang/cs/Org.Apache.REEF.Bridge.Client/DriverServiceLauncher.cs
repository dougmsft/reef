// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using Google.Protobuf;

using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Bridge.Proto;

namespace Org.Apache.REEF.Bridge.Client
{
    /// <summary>
    /// Proxy class that launches the Java driver service launcher.
    /// </summary>
    class DriverServiceLauncher 
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DriverServiceLauncher));


        public static void Submit(DriverClientConfiguration config)
        {
            using (StreamWriter writer = new StreamWriter("DriverServiceLauncher.cfg"))
            {
                JsonFormatter formatter = JsonFormatter.Default;
                formatter.Format(config, writer);
            }

            // Setup the conversion process.
            ProcessStartInfo rProcInfo = new ProcessStartInfo()
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
                FileName = "java.exe" // Path.Combine(grpcToolsDirectory ,"protoc.exe")
            };

            StringBuilder stdOutBuilder = new StringBuilder();
            stdOutBuilder.AppendLine("[StdOut]: ");
            StringBuilder stdErrBuilder = new StringBuilder();
            stdErrBuilder.AppendLine("[StdErr]: ");

            // Start the conversion process
            using (Process rProc = Process.Start(rProcInfo))
            {
                // Read the standard out and error on separate threads
                // simultaneously to avoid deadlock when rProc fills one
                // of the buffers and waits for this process to read it.
                // Use fully qualified namespace to avoid conflict
                // with Task class defined in visual studio.
                var stdOutTask = System.Threading.Tasks.Task.Run(
                  () => stdOutBuilder.Append(rProc.StandardOutput.ReadToEnd()));
                var stdErrTask = System.Threading.Tasks.Task.Run(
                  () => stdErrBuilder.Append(rProc.StandardError.ReadToEnd()));

                rProc.WaitForExit();
                if (rProc.ExitCode != 0)
                {
                    Logger.Log(Level.Error, "Exec of DriverServiceLauncher failed");
                    //throw new Exception("DriverServiceLauncher failed");
                }

                // Wait for std out and error readers.
                stdOutTask.Wait();
                stdErrTask.Wait();
            }
            Logger.Log(Level.Info, stdOutBuilder.ToString());
            Logger.Log(Level.Info, stdErrBuilder.ToString());
        }

        private DriverServiceLauncher()
        {
        }
    }
}
