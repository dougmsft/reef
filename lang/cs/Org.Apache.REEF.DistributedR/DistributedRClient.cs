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
using System.Net;
using System.Globalization;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.HDI;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.DistributedR
{
    public sealed class ClientOptions
    {
        [NamedParameter(Documentation = "Client to driver network")]
        public class Network : Name<MessageService>
        {
        }

        public sealed class ModuleBuilder : ConfigurationModuleBuilder
        {
            public static readonly RequiredParameter<MessageService> Network = new RequiredParameter<MessageService>();

            public static readonly ConfigurationModule Config = new ModuleBuilder()
                .BindNamedParameter(GenericType<ClientOptions.Network>.Class, Network)
                .Build();
        } 
    }

    /// <summary>
    /// A Tool that submits DistributedRDriver for execution.
    /// </summary>
    public sealed class DistributedRClient
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(DistributedRClient));

        static private INameServer sNAME_SERVER;
        static private string sCLIENT_ID;
        static private string sDRIVER_ID;

        private const string Local = "local";
        private const string YARN = "yarn";
        private const string YARNRest = "yarnrest";
        private const string HDInsight = "hdi";

        private readonly IREEFClient reefClient;
        private readonly JobRequestBuilder jobRequestBuilder;
        private readonly MessageService network;

        private enum NetworkType
        {
            Client,
            Driver
        }

        static DistributedRClient()
        {
            sNAME_SERVER = TangFactory.GetTang().NewInjector().GetInstance<INameServer>();

            // Create service identifiers for the client and driver.
            string guidStr = Guid.NewGuid().ToString();
            sCLIENT_ID = "DistRClient-" + guidStr;
            sDRIVER_ID = "DistRDriver-" + guidStr;
        }

        [Inject]
        private DistributedRClient(
            [Parameter(typeof(ClientOptions.Network))] MessageService network,
            IREEFClient reefClient,
            JobRequestBuilder jobRequestBuilder)
        {
            this.network = network;
            this.reefClient = reefClient;
            this.jobRequestBuilder = jobRequestBuilder;
        }

        public void runRScript(string script)
        {
            this.network.Send(script);
        }

        private IConfiguration BuildDriverConfiguration()
        {
            IConfiguration networkConfig = GetNetworkConfiguration(NetworkType.Driver);

            IConfiguration driverConfig = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<DistributedRDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<DistributedRDriver>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<DistributedRDriver>.Class)
                .Set(DriverConfiguration.OnTaskMessage, GenericType<DistributedRDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<DistributedRDriver>.Class)
                .Build();

            IConfiguration driverNetworkConfig = DriverOptions.ModuleBuilder.Config
                .Set(DriverOptions.ModuleBuilder.Network, GenericType<MessageService>.Class)
                .Build();

            return Configurations.Merge(networkConfig, driverNetworkConfig, driverConfig);
        }

        private JobRequest BuildJobRequest()
        {
            return jobRequestBuilder
                .AddDriverConfiguration(BuildDriverConfiguration())
                .AddGlobalAssemblyForType(typeof(DistributedRDriver))
                .SetJobIdentifier("DistributedRApp")
                .Build();
        }

        /// <summary>
        /// Runs DistributedRApp using the IREEFClient passed into the constructor.
        /// </summary>
        private void Run()
        {
            reefClient.Submit(BuildJobRequest());

            this.network.Start();
            this.network.Send("writeMessage <- function() {\n print('TEST SCRIPT')\n }\n writeMessage()\n Sys.info()\n");

            // Wait for the results.
            this.network.Stop();
        }

        /// <summary>
        /// Set the local runtime where the driver and evaluators will run.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfiguration(string name)
        {
            switch (name)
            {
                case Local:
                    return LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "4")
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

        private static IConfiguration GetNetworkConfiguration(NetworkType networkType)
        {
             IPEndPoint endpoint = sNAME_SERVER.LocalEndpoint;

            string serviceIdentifier = sCLIENT_ID;
            string remoteServiceIdentifier = sDRIVER_ID;

            int servicePort = 49499;
            if (networkType == NetworkType.Driver)
            {
                servicePort = 51511;
                serviceIdentifier = sDRIVER_ID;
                remoteServiceIdentifier = sCLIENT_ID;
            }

            IConfiguration nameClientConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                    GenericType<NamingConfigurationOptions.NameServerPort>.Class,
                    endpoint.Port.ToString(CultureInfo.CurrentCulture))
                .BindNamedParameter<NamingConfigurationOptions.NameServerAddress, string>(
                    GenericType<NamingConfigurationOptions.NameServerAddress>.Class, endpoint.Address.ToString())
                .BindImplementation(GenericType<INameClient>.Class, GenericType<NameClient>.Class)
                .Build();

            IConfiguration networkServiceConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindIntNamedParam<NetworkServiceOptions.NetworkServicePort>(servicePort.ToString())
                .Build();

            IConfiguration msgServiceConfig = NetworkOptions.ModuleBuilder.Config
                .Set(NetworkOptions.ModuleBuilder.NetService, GenericType<NetworkService<string>>.Class)
                .Set(NetworkOptions.ModuleBuilder.ServiceIdentifier, serviceIdentifier)
                .Set(NetworkOptions.ModuleBuilder.RemoteServiceIdentifier, remoteServiceIdentifier)
                .Set(NetworkOptions.ModuleBuilder.MessageHandler, GenericType<MessageHandler>.Class)
                .Set(NetworkOptions.ModuleBuilder.MessageHandlerImpl, GenericType<MessageHandler>.Class)
                .Set(NetworkOptions.ModuleBuilder.CodecImpl, GenericType<StringCodec>.Class)
                .Build();

            return Configurations.Merge(nameClientConfig, networkServiceConf, msgServiceConfig);
        }

        public static void Main(string[] args)
        {
            IConfiguration networkConfig = GetNetworkConfiguration(NetworkType.Client);

            IConfiguration clientConfig = ClientOptions.ModuleBuilder.Config
                .Set(ClientOptions.ModuleBuilder.Network, GenericType<MessageService>.Class)
                .Build();

            TangFactory.GetTang().NewInjector(GetRuntimeConfiguration(args.Length > 0 ? args[0] : Local),
                networkConfig, clientConfig).GetInstance<DistributedRClient>().Run();
        }
    }
}