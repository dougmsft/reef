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
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.HDI;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.DistributedR
{
    /// <summary>
    /// A Tool that submits DistributedRDriver for execution.
    /// </summary>
    public sealed class DistributedRApp
    {
        private const string Local = "local";
        private const string YARN = "yarn";
        private const string YARNRest = "yarnrest";
        private const string HDInsight = "hdi";
        private readonly IREEFClient _reefClient;
        private readonly JobRequestBuilder _jobRequestBuilder;

        [Inject]
        private DistributedRApp(IREEFClient reefClient, JobRequestBuilder jobRequestBuilder)
        {
            _reefClient = reefClient;
            _jobRequestBuilder = jobRequestBuilder;
        }

        /// <summary>
        /// Runs DistributedRApp using the IREEFClient passed into the constructor.
        /// </summary>
        private void Run()
        {
            // The driver configuration contains all the needed bindings.
            var distRDriverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<DistributedRDriver>.Class)
                .Set(DriverConfiguration.OnDriverStarted, GenericType<DistributedRDriver>.Class)
                .Build();

            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var distRJobRequest = _jobRequestBuilder
                .AddDriverConfiguration(distRDriverConfiguration)
                .AddGlobalAssemblyForType(typeof(DistributedRDriver))
                .SetJobIdentifier("DistributedRApp")
                .Build();

            _reefClient.Submit(distRJobRequest);
        }

        /// <summary>
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfiguration(string name)
        {
            switch (name)
            {
                case Local:
                    return LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "2")
                        .Build();
                case YARN:
                    return YARNClientConfiguration.ConfigurationModule.Build();
                case YARNRest:
                    return YARNClientConfiguration.ConfigurationModuleYARNRest.Build();
                case HDInsight:
                    // To run against HDInsight please replace placeholders below, with actual values for
                    // connection string, container name (available at Azure portal) and HDInsight 
                    // credentials (username and password)
                    const string connectionString = "ConnString";
                    const string continerName = "foo";
                    return HDInsightClientConfiguration.ConfigurationModule
                        .Set(HDInsightClientConfiguration.HDInsightPasswordParameter, @"!12345ms54321!")
                        .Set(HDInsightClientConfiguration.HDInsightUsernameParameter, @"sshuser")
                        .Set(HDInsightClientConfiguration.HDInsightUrlParameter, @"https://exreefhdi.azurehdinsight.net")
                        .Set(HDInsightClientConfiguration.JobSubmissionDirectoryPrefix, string.Format(@"/{0}/tmp", continerName))
                        .Set(AzureBlockBlobFileSystemConfiguration.ConnectionString, connectionString)
                        .Build();
                default:
                    throw new Exception("Unknown runtime: " + name);
            }
        }

        public static void Main(string[] args)
        {
            TangFactory.GetTang().NewInjector(GetRuntimeConfiguration(args.Length > 0 ? args[0] : Local)).GetInstance<Org.Apache.REEF.DistributedR.DistributedRApp>().Run();
        }
    }
}