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

using System.Threading;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.DistributedR
{
    public sealed class DriverProxyOptions
    {
        [NamedParameter(Documentation = "Driver configuration")]
        public class ConfigProvider : Name<DriverConfigurationProvider>
        {
        }

        public sealed class ModuleBuilder : ConfigurationModuleBuilder
        {
            public static readonly RequiredParameter<DriverConfigurationProvider> ConfigProvider =
                new RequiredParameter<DriverConfigurationProvider>();

            public static readonly ConfigurationModule Config = new ModuleBuilder()
                .BindNamedParameter(GenericType<DriverProxyOptions.ConfigProvider>.Class, ConfigProvider)
                .Build();
        } 
    }

    /// <summary>
    /// The DriverProxy class is responsible for launching the driver.
    /// </summary>
    class DriverProxy
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(DriverProxy));

        // Driver
        private readonly DriverConfigurationProvider configurationProvider;
        private readonly IREEFClient reefClient;
        private readonly JobRequestBuilder jobRequestBuilder;

        private CancellationTokenSource cancelSource;
        private CancellationToken cancelToken;
        private Thread driver;

        [Inject]
        private DriverProxy(
            [Parameter(typeof(DriverProxyOptions.ConfigProvider))] DriverConfigurationProvider configurationProvider,
            IREEFClient reefClient,
            JobRequestBuilder jobRequestBuilder)
        {
            this.configurationProvider = configurationProvider;
            this.reefClient = reefClient;
            this.jobRequestBuilder = jobRequestBuilder;
        }

        private JobRequest BuildJobRequest()
        {
            return jobRequestBuilder
                .AddDriverConfiguration(this.configurationProvider.GetConfiguration())
                .AddGlobalAssemblyForType(typeof(DriverHandler))
                .SetJobIdentifier("DistributedR")
                .Build();
        }

        /// <summary>
        /// Launch the Distributed R driver and block until it exits.
        /// </summary>
        public void Run()
        {
            Logr.Log(Level.Info, "Starting Distributed R driver proxy");

            this.cancelSource = new CancellationTokenSource();
            this.cancelToken = this.cancelSource.Token;
            this.driver = new Thread(new ThreadStart(this.InstantiateDriver));
            this.driver.Start();
        }

        private void InstantiateDriver()
        {
            reefClient.Submit(BuildJobRequest());
        }
    }
}
