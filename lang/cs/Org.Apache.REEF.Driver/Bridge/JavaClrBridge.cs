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
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Local;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Driver.Bridge
{
    class JavaClrBridge : IObserver<string>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(JavaClrBridge));

        private IRemoteManager<string> remoteManager;
        private BlockingCollection<string> queue = new BlockingCollection<string>();

        public JavaClrBridge(ILocalAddressProvider localAddressProvider)
        {
            // Instantiate a file system proxy.
            IFileSystem fileSystem = TangFactory.GetTang()
                .NewInjector(LocalFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IFileSystem>();

            // Get the path to the bridge name server endpoint file.
            string javaBridgeAddress = null;
            REEFFileNames fileNames = new REEFFileNames();
            using (FileStream stream = File.Open(fileNames.GetDriverNameServerEndpoint(), FileMode.Open))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    javaBridgeAddress = reader.ReadToEnd();
                }
            }
            Logger.Log(Level.Info, string.Format("Name Server Address: {0}", (javaBridgeAddress == null) ? "NULL" : javaBridgeAddress));

            BuildRemoteManager(localAddressProvider, javaBridgeAddress);
        }

        public void OnNext(string value)
        {
            Logger.Log(Level.Info, "JavaCLRBridge received message: [" + value + "]");
        }

        public void OnError(Exception error)
        {
            Logger.Log(Level.Info, "JavaCLRBridge error: [" + error.Message + "]");
        }

        public void OnCompleted()
        {
            Logger.Log(Level.Info, "JavaCLRBridge OnCompleted");
        }

        private void BuildRemoteManager(
            ILocalAddressProvider localAddressProvider,
            string javaBridgeAddress)
        {
            IRemoteManagerFactory remoteManagerFactory =
                TangFactory.GetTang().NewInjector().GetInstance<IRemoteManagerFactory>();

            remoteManager = remoteManagerFactory.GetInstance(localAddressProvider.LocalAddress, new StringCodec());
            Logger.Log(Level.Info, string.Format("Starting bridge connector on: [{0}]", remoteManager.LocalEndpoint));

            string[] addressStrs = javaBridgeAddress.Split(':');
            IPAddress javaBridgeIpAddress = IPAddress.Parse(addressStrs[0]);
            int port = int.Parse(addressStrs[1]);

            IPEndPoint ipEndPoint = new IPEndPoint(javaBridgeIpAddress, port);
            remoteManager.RegisterObserver(ipEndPoint, this);
            Logger.Log(Level.Info, string.Format("Listening java bridge on: [{0}]", ipEndPoint.ToString()));
        }
    }
}
