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
    public sealed class JuliaExecTask : ITask
    {
        private static readonly Logger LOG = Logger.GetLogger(typeof(JuliaExecTask));
        private readonly string _function;
        private readonly string _data;

        [Inject]
        private JuliaExecTask(
            [Parameter(typeof(JuliaExecTaskOptions.Function))] string function,
            [Parameter(typeof(JuliaExecTaskOptions.Data))] string data)
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
            LOG.Log(Level.Info, "Fluid: Starting Julia");

            WaitForDebugger();

            string stdOutStr = null;
            string stdErrorStr = null;
            try
            {
                ProcessStartInfo juliaProcInfo = new ProcessStartInfo()
                {
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardInput = true,
                    RedirectStandardError = true,

                    // Need general method for path for both windows and linux.
                    FileName = @"C:\Program Files\Julia\Julia-0.6.0\bin\Julia.exe",
                    Arguments = string.Empty 
                };

                int exitCode;
                using (Process juliaProc = Process.Start(juliaProcInfo))
                {
                    // Inject the script into the R interpreter.
                    StreamWriter writer = juliaProc.StandardInput;
                    writer.WriteLine(_function);
                    writer.WriteLine("quit()");

                    // Capture the output from the interpreter.
                    StringBuilder stdOutBuilder = new StringBuilder();
                    StringBuilder stdErrorBuilder = new StringBuilder();

                    stdOutBuilder.Append(juliaProc.StandardOutput.ReadToEnd());
                    stdErrorBuilder.Append(juliaProc.StandardError.ReadToEnd());

                    // Wait for Julia to exit.
                    juliaProc.WaitForExit();
                    exitCode = juliaProc.ExitCode;

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