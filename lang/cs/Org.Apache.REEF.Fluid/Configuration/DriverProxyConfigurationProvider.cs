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
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.HDI;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Fluid
{
    public sealed class DriverProxyConfigProviderOptions
    {
        [NamedParameter(Documentation = "Runtime type")]
        public class RuntimeType : Name<string>
        {
        }

        public sealed class ModuleBuilder : ConfigurationModuleBuilder
        {
            public static readonly RequiredParameter<string> RuntimeType = new RequiredParameter<string>();
            public static readonly ConfigurationModule Config = new ModuleBuilder()
                .BindNamedParameter(GenericType<DriverProxyConfigProviderOptions.RuntimeType>.Class, RuntimeType)
                .Build();
        } 
    }

    public enum Runtime
    {
        Local,
        Yarn,
        YarnRest,
        HDInsight
    }

    public class DriverProxyConfigurationProvider : IConfigurationProvider
    {
        private readonly string runtime;

        [Inject]
        private DriverProxyConfigurationProvider(
            [Parameter(typeof(DriverProxyConfigProviderOptions.RuntimeType))] string runtimeType)
        {
            this.runtime = runtimeType;
        }

        /// <summary>
        /// Set the runtime where the driver and evaluators will run.
        /// </summary>
        /// <returns>An object that implements IConfiguration and contains driver proxy configuration.</returns>
        public IConfiguration GetConfiguration()
        {
            IConfiguration proxyConfig = DriverProxyOptions.ModuleBuilder.Config
                .Set(DriverProxyOptions.ModuleBuilder.ConfigProvider, GenericType<DriverConfigurationProvider>.Class)
                .Build();

            IConfiguration runtimeConfig;
            switch (this.runtime)
            {
                case "Local":
                    runtimeConfig = LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "4")
                        .Build();
                    break;
                case "Yarn":
                    runtimeConfig = YARNClientConfiguration.ConfigurationModule.Build();
                    break;
                case "YarnRest":
                    runtimeConfig = YARNClientConfiguration.ConfigurationModuleYARNRest.Build();
                    break;
                case "HDInsight":
                    // To run against HDInsight please replace placeholders below, with actual values for
                    // connection string, container name (available at Azure portal) and HDInsight 
                    // credentials (username and password)
                    const string connectionString = "ConnString";
                    const string continerName = "foo";
                    runtimeConfig = HDInsightClientConfiguration.ConfigurationModule
                        .Set(HDInsightClientConfiguration.HDInsightPasswordParameter, @"!12345ms54321!")
                        .Set(HDInsightClientConfiguration.HDInsightUsernameParameter, @"sshuser")
                        .Set(HDInsightClientConfiguration.HDInsightUrlParameter, @"https://exreefhdi.azurehdinsight.net")
                        .Set(HDInsightClientConfiguration.JobSubmissionDirectoryPrefix, string.Format(@"/{0}/tmp", continerName))
                        .Set(AzureBlockBlobFileSystemConfiguration.ConnectionString, connectionString)
                        .Build();
                    break;
                default:
                    throw new Exception("Unknown runtime: " + runtime.ToString());
            }

            return Configurations.Merge(proxyConfig, runtimeConfig);
        }
    }
}
