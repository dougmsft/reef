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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Fluid
{
    /// <summary>
    /// A Task that merely prints a greeting and exits.
    /// </summary>
    public sealed class RExecTask : ITask
    {
        private static readonly Logger LOG = Logger.GetLogger(typeof(RExecTask));
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
            LOG.Log(Level.Info, "Disposed.");
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
            LOG.Log(Level.Info, "Fluid: Starting R");

            WaitForDebugger();

            string stdOutStr = null;
            string stdErrorStr = null;
            try
            {
                ProcessStartInfo rProcInfo = new ProcessStartInfo()
                {
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardInput = true,
                    RedirectStandardError = true,

                    // Need general method for path for both windows and linux.
                    // FileName = "C:\\Program Files\\Microsoft\\R Client\\R_SERVER\\bin\\R.exe",
                    FileName = @"C:\Program Files\R\R-3.4.1\bin\R.exe",
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
                    StringBuilder stdOutBuilder = new StringBuilder();
                    StringBuilder stdErrorBuilder = new StringBuilder();
                    stdOutBuilder.Append(rProc.StandardOutput.ReadToEnd());

                    rProc.WaitForExit();
                    exitCode = rProc.ExitCode;

                    stdOutStr = stdOutBuilder.ToString();
                    stdErrorStr = stdErrorBuilder.ToString();
                }
                LOG.Log(Level.Info, "Exit Code: {0}", exitCode);
                LOG.Log(Level.Info, "Standard Out: {0}", stdOutStr);
                LOG.Log(Level.Info, "Standard Error: {0}", stdErrorStr);
            }
            catch (Exception except)
            {
                stdOutStr = string.Format(CultureInfo.InvariantCulture, "Failed to execute R: [{0}] {1}", except, except.Message);
                stdErrorStr = stdOutStr;
            }

            return ByteUtilities.StringToByteArrays("STD OUT:\n" + stdOutStr + "\nSTD ERROR: \n" + stdErrorStr);
        }
    }
}