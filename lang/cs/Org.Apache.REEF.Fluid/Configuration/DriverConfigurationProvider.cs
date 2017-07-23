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

using Org.Apache.REEF.Fluid.Network;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Fluid
{
    public class DriverConfigurationProvider : IConfigurationProvider
    {
        [Inject]
        protected DriverConfigurationProvider()
        {
        }

        public IConfiguration GetConfiguration()
        {
            IConfiguration netConfigProviderCofng = NetConfigProviderOptions.ModuleBuilder.Config
                .Set(NetConfigProviderOptions.ModuleBuilder.NetType, NetworkType.Driver.ToString())
                .Build();
            NetworkConfigurationProvider configProvider =
                TangFactory.GetTang().NewInjector(netConfigProviderCofng).GetInstance<NetworkConfigurationProvider>();

            IConfiguration networkConfig = configProvider.GetConfiguration();

            IConfiguration driverConfig = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<DriverHandler>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<DriverHandler>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<DriverHandler>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<DriverHandler>.Class)
                .Set(DriverConfiguration.OnTaskMessage, GenericType<DriverHandler>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<DriverHandler>.Class)
                .Build();

            IConfiguration driverNetworkConfig = DriverOptions.ModuleBuilder.Config
                .Set(DriverOptions.ModuleBuilder.Network, GenericType<MessageService>.Class)
                .Build();

            return Configurations.Merge(networkConfig, driverNetworkConfig, driverConfig);
        }
    }
}
