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

using Org.Apache.REEF.Fluid.Message;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Fluid
{
    class FluidApplication
    {
        private static readonly Logger LOG = Logger.GetLogger(typeof(FluidApplication));
        private TaskRunner Runner { get; }

        public FluidApplication()
        {
            // Configure and launch the job runner.
            Runner = new TaskRunner();
        }

        public void Initialize()
        {
            // Configure and launch the driver.
            IConfiguration proxyConfigProviderConfig = DriverProxyConfigProviderOptions.ModuleBuilder.Config
                .Set(DriverProxyConfigProviderOptions.ModuleBuilder.RuntimeType, Runtime.Local.ToString())
                .Build();
            DriverProxyConfigurationProvider configProvider =
                TangFactory.GetTang().NewInjector(proxyConfigProviderConfig).GetInstance<DriverProxyConfigurationProvider>();
            DriverProxy driverProxy =
                TangFactory.GetTang().NewInjector(configProvider.GetConfiguration()).GetInstance<DriverProxy>();
            driverProxy.Run();

            // Start message processing.
            Runner.Start();
        }

        public void Deinitialize()
        {
            Runner.Stop();
        }

        private static void Main(string[] args)
        {
            FluidApplication distR = new FluidApplication();
            distR.Initialize();
            TaskRunner runner = distR.Runner;

            ///////////////////////
            RTaskMsg rtask1 = new RTaskMsg(Guid.NewGuid().ToString(), "doIt1 <- function(x = 'NOTHING') { return x }\nSys.sleep(2)", "TheData");
            RTaskMsg rtask2 = new RTaskMsg(Guid.NewGuid().ToString(), "doIt2 <- function(x = 'SOMETHING') { return x }\nSys.sleep(2)", "TheData");
            RTaskMsg rtask3 = new RTaskMsg(Guid.NewGuid().ToString(), "doIt3 <- function(x = 'EVERYTHING') { return x }\nSys.sleep(2)", "TheData");
            RTaskMsg rtask4 = new RTaskMsg(Guid.NewGuid().ToString(), "doIt4 <- function(x = 'Four') { return x }\nSys.sleep(2)", "TheData");
            RTaskMsg rtask5 = new RTaskMsg(Guid.NewGuid().ToString(), "doIt5 <- function(x = 'Five') { return x }\nSys.sleep(2)", "TheData");
            RTaskMsg rtask6 = new RTaskMsg(Guid.NewGuid().ToString(), "doIt6 <- function(x = 'Six') { return x }\nSys.sleep(2)", "TheData");

            runner.Submit(rtask1);
            runner.Submit(rtask2);
            runner.Submit(rtask3);
            runner.Submit(rtask4);
            runner.Submit(rtask5);
            runner.Submit(rtask6);

            JuliaTaskMsg juliaTask1 = new JuliaTaskMsg(Guid.NewGuid().ToString(), "function doIt1(x) x end\nprintln(\"SLEEPING\")\nsleep(2)", "TheData1");
            JuliaTaskMsg juliaTask2 = new JuliaTaskMsg(Guid.NewGuid().ToString(), "function doIt2(x) x end\nprintln(\"SLEEPING\")\nsleep(2)", "TheData2");
            JuliaTaskMsg juliaTask3 = new JuliaTaskMsg(Guid.NewGuid().ToString(), "function doIt3(x) x end\nprintln(\"SLEEPING\")\nsleep(2)", "TheData3");
            JuliaTaskMsg juliaTask4 = new JuliaTaskMsg(Guid.NewGuid().ToString(), "function doIt4(x) x end\nprintln(\"SLEEPING\")\nsleep(2)", "TheData4");
            JuliaTaskMsg juliaTask5 = new JuliaTaskMsg(Guid.NewGuid().ToString(), "function doIt5(x) x end\nprintln(\"SLEEPING\")\nsleep(2)", "TheData5");
            JuliaTaskMsg juliaTask6 = new JuliaTaskMsg(Guid.NewGuid().ToString(), "function doIt6(x) x end\nprintln(\"SLEEPING\")\nsleep(2)", "TheData6");

            runner.Submit(juliaTask1);
            runner.Submit(juliaTask2);
            runner.Submit(juliaTask3);
            runner.Submit(juliaTask4);
            runner.Submit(juliaTask5);
            runner.Submit(juliaTask6);

            ///////////////////////
            object message;
            for (int idx = 0; idx < 12; ++idx)
            {
                message = runner.GetResultsBlocking();
                LOG.Log(Level.Info, "Received task results: " + message.ToString());
            }

            distR.Deinitialize();
        }
    }
}
