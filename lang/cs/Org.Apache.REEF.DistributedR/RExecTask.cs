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
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.DistributedR
{
    /// <summary>
    /// A Task that merely prints a greeting and exits.
    /// </summary>
    public sealed class RExecTask : ITask
    {
        private readonly string _function;
        private readonly string _data;

        [Inject]
        private RExecTask(
            [Parameter(typeof(RExecTaskOptions.Function))] string function,
            [Parameter(typeof(RExecTaskOptions.Data))] string data)
        {
            _function = function;
            _data = data;
        }

        public void Dispose()
        {
            Console.WriteLine("Disposed.");
        }

        private void WaitForDebugger()
        {
            bool doWait = false;
            while (doWait)
            {
                Thread.Sleep(1000);
            }
        }

        public byte[] Call(byte[] memento)
        {
            Console.WriteLine("DistributedR: Starting R");

            WaitForDebugger();

            string stdOutStr;
            try
            {
                ProcessStartInfo rProcInfo = new ProcessStartInfo()
                {
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardInput = true,

                    // Need general method for path for both windows and linux.
                    // FileName = "C:\\Program Files\\Microsoft\\R Client\\R_SERVER\\bin\\R.exe",
                    FileName = @"C:\Program Files\R\R-3.3.3\bin\R.exe",
                    Arguments = "--no-save"
                };

                int exitCode;
                using (Process rProc = Process.Start(rProcInfo))
                {
                    // Inject the script into the R interpreter.
                    StreamWriter writer = rProc.StandardInput;
                    writer.WriteLine(_function);
                    writer.WriteLine("q()");

                    // Capture the output from the interpreter.
                    // INCLUDE STD ERROR.
                    StringBuilder stdOutBuilder = new StringBuilder();
                    stdOutBuilder.Append(rProc.StandardOutput.ReadToEnd());
                    rProc.WaitForExit();
                    exitCode = rProc.ExitCode;
                    stdOutStr = stdOutBuilder.ToString();
                }
                Console.WriteLine("Exit Code: {0}", exitCode);
                Console.WriteLine("Standard Out: {0}", stdOutStr);
            }
            catch (Exception except)
            {
                stdOutStr = string.Format(CultureInfo.InvariantCulture, "Failed to execute R: [{0}] {1}", except, except.Message);
            }

            Thread.Sleep(2000);
            return ByteUtilities.StringToByteArrays(stdOutStr);
        }
    }
}