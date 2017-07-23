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
// under the License

using System;
using System.Net;
using System.Globalization;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Fluid.Network;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Fluid
{
    public sealed class NetConfigProviderOptions
    {
        [NamedParameter(Documentation = "Network type")]
        public class NetType : Name<string>
        {
        }

        public sealed class ModuleBuilder : ConfigurationModuleBuilder
        {
            public static readonly RequiredParameter<string> NetType = new RequiredParameter<string>();
            public static readonly ConfigurationModule Config = new ModuleBuilder()
                .BindNamedParameter(GenericType<NetConfigProviderOptions.NetType>.Class, NetType)
                .Build();
        } 
    }

    public enum NetworkType
    {
        Client,
        Driver
    }

    class NetworkConfigurationProvider : IConfigurationProvider
    {
        // Network related statics variables.
        public static readonly INameServer NAME_SERVER;
        public static readonly string CLIENT_ID;
        public static readonly string DRIVER_ID;

        public readonly NetworkType networkType;

        /// <summary>
        /// Instantiate the nameserver and generates client and driver network identifiers.
        /// </summary>
        static NetworkConfigurationProvider()
        {
            // Instantiate the name server.
            NAME_SERVER = TangFactory.GetTang().NewInjector().GetInstance<INameServer>();

            // Create service identifiers for the client and driver.
            string guidStr = Guid.NewGuid().ToString();
            CLIENT_ID = "DistRClient-" + guidStr;
            DRIVER_ID = "DistRDriver-" + guidStr;
        }

        [Inject]
        private NetworkConfigurationProvider(
        [Parameter(typeof(NetConfigProviderOptions.NetType))] string networkType)
        {
            // This is error prone add support to configuration module for enumerated types and integers. 
            this.networkType = (networkType == "Client") ? NetworkType.Client : NetworkType.Driver;
        }

        public IConfiguration GetConfiguration()
        {
            IPEndPoint endpoint = NAME_SERVER.LocalEndpoint;

            string serviceIdentifier = CLIENT_ID;
            string remoteServiceIdentifier = DRIVER_ID;

            int servicePort = 49499;
            if (networkType == NetworkType.Driver)
            {
                servicePort = 58228;
                serviceIdentifier = DRIVER_ID;
                remoteServiceIdentifier = CLIENT_ID;
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

            IConfiguration msgServiceConfig = MessageServiceOptions.ModuleBuilder.Config
                .Set(MessageServiceOptions.ModuleBuilder.NetService, GenericType<NetworkService<byte[]>>.Class)
                .Set(MessageServiceOptions.ModuleBuilder.ServiceIdentifier, serviceIdentifier)
                .Set(MessageServiceOptions.ModuleBuilder.RemoteServiceIdentifier, remoteServiceIdentifier)
                .Set(MessageServiceOptions.ModuleBuilder.MessageHandler, GenericType<MessageHandler>.Class)
                .Set(MessageServiceOptions.ModuleBuilder.MessageHandlerImpl, GenericType<MessageHandler>.Class)
                .Set(MessageServiceOptions.ModuleBuilder.CodecImpl, GenericType<ByteCodec>.Class)
                .Build();

            return Configurations.Merge(nameClientConfig, networkServiceConf, msgServiceConfig);
        }
    }
}
