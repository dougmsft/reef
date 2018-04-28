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

using System.Diagnostics;
using Org.Apache.REEF.Bridge.Proto;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Client.Examples.Hello
{
    /// <summary>
    /// Client class for Hello REEF example.
    /// </summary>
    public sealed class HelloREEF
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HelloREEF));
        private static TextWriterTraceListener listener = new TextWriterTraceListener("Hello.log");

        /// Hello REEF Driver configuration.
        IConfiguration driverConfig = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<HelloDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<HelloDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<HelloDriver>.Class)
                .Set(DriverConfiguration.CustomTraceLevel, Level.Verbose.ToString())
                .Build();

        /// <summary>
        /// Run HelloREEF on the local runtime.1
        /// </summary>
        /// <param name="args">command line parameters</param>
        static void Main(string[] args)
        {
            Logger.AddTraceListener(listener);

            DriverClientConfiguration config = new DriverClientConfiguration();
            config.Jobid = "HelloREEF";
            config.LocalRuntime = new LocalRuntimeParameters();
            config.LocalRuntime.MaxNumberOfEvaluators = 1;
            config.GlobalLibraries.Add(typeof(HelloREEF).Assembly.FullName);

            DriverServiceLauncher.Submit(config);
            Logger.Log(Level.Info, "REEF job completed");
            listener.Flush();
        }

        private HelloREEF()
        {
        }
    }
}