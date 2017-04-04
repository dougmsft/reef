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
using System.Threading;
using Org.Apache.REEF.DistributedR.Network;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.DistributedR
{
    class DistR
    {
        TaskRunner Runner { get; }

        public DistR()
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
            Logger _logr = Logger.GetLogger(typeof(DistR));

            DistR distR = new DistR();
            distR.Initialize();
            TaskRunner runner = distR.Runner;

            RTaskMsg rtask1 = RTaskMsg.Factory("doIt1 <- function(x = 'NOTHING') { return x }", "TheData");
            RTaskMsg rtask2 = RTaskMsg.Factory("doIt2<- function(x = 'SOMETHING') { return x }", "TheData");
            RTaskMsg rtask3 = RTaskMsg.Factory("doIt3<- function(x = 'EVERYTHING') { return x }", "TheData");
            RTaskMsg rtask4 = RTaskMsg.Factory("doIt4<- function(x = 'Four') { return x }", "TheData");
            RTaskMsg rtask5 = RTaskMsg.Factory("doIt5<- function(x = 'Five') { return x }", "TheData");
            RTaskMsg rtask6 = RTaskMsg.Factory("doIt6<- function(x = 'Six') { return x }", "TheData");

            runner.Submit(rtask1);
            runner.Submit(rtask2);
            runner.Submit(rtask3);
            runner.Submit(rtask4);
            runner.Submit(rtask5);
            runner.Submit(rtask6);

            IMessage message;
            for (int idx = 0; idx < 6; ++idx)
            {
                message = runner.GetResultsBlocking();
                _logr.Log(Level.Info, "Received task results: " + message.ToString());
            }

            distR.Deinitialize();
        }
    }
}
