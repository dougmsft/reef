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
using System.IO;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using System.Diagnostics;

namespace Org.Apache.REEF.DistributedR
{
    /// <summary>
    /// A Task that merely prints a greeting and exits.
    /// </summary>
    public sealed class DistributedRTask : ITask
    {
        [Inject]
        private DistributedRTask()
        {
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

            ProcessStartInfo rProcInfo = new ProcessStartInfo();
            rProcInfo.UseShellExecute = false;
            rProcInfo.RedirectStandardOutput = true;
            rProcInfo.RedirectStandardInput = true;
            rProcInfo.FileName = "C:\\Program Files\\R\\R-3.3.3\\bin\\R.exe";
            rProcInfo.Arguments = "--no-save";

            int exitCode;
            string stdOutStr;
            using (Process rProc = Process.Start(rProcInfo))
            {
                StreamWriter writer = rProc.StandardInput;
                writer.WriteLine("Sys.info()");
                writer.WriteLine("q()");
                stdOutStr = rProc.StandardOutput.ReadToEnd();
                rProc.WaitForExit();
                exitCode = rProc.ExitCode;
            }
            Console.WriteLine("Exit Code: {0}", exitCode);
            Console.WriteLine("Standard Out: {0}", stdOutStr);
            return null;
        }
    }
}