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
using org.apache.reef.bridge.message;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Local;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Bridge
{
    public class JavaClrInterop : IObserver<IRemoteMessage<byte[]>>, IObserver<SystemOnStart>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(JavaClrInterop));

        private IRemoteManager<byte[]> remoteManager;
        private IObserver<byte[]> remoteObserver;
        private BlockingCollection<byte[]> queue = new BlockingCollection<byte[]>();

        [Inject]
        private JavaClrInterop(ILocalAddressProvider localAddressProvider)
        {
            // Instantiate a file system proxy.
            IFileSystem fileSystem = TangFactory.GetTang()
                .NewInjector(LocalFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IFileSystem>();

            // Get the path to the bridge name server endpoint file.
            string javaBridgeAddress = null;
            REEFFileNames fileNames = new REEFFileNames();
            using (FileStream stream = File.Open(fileNames.GetDriverJavaBridgeEndpoint(), FileMode.Open))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    javaBridgeAddress = reader.ReadToEnd();
                }
            }
            Logger.Log(Level.Info, string.Format("Name Server Address: {0}", (javaBridgeAddress == null) ? "NULL" : javaBridgeAddress));

            BuildRemoteManager(localAddressProvider, javaBridgeAddress);
        }

        public void OnNext(IRemoteMessage<byte[]> message)
        {
            Logger.Log(Level.Info, "++++++JavaCLRBridge received message: " + message.Identifier.ToString());
            Serializer.Read(message.Message, this);
        }

        public void OnNext(SystemOnStart systemOnStart)
        {
            Logger.Log(Level.Info, "++++++JavaCLRBridge received SystemOnStart message: " + systemOnStart.dateTime);
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
            string javaBridgeAddrStr)
        {
            // Instantiate the remote manager.
            IRemoteManagerFactory remoteManagerFactory =
                TangFactory.GetTang().NewInjector().GetInstance<IRemoteManagerFactory>();
            remoteManager = remoteManagerFactory.GetInstance(localAddressProvider.LocalAddress, new ByteCodec());

            // Listen to the java bridge on the local end point.
            remoteManager.RegisterObserver(this);
            Logger.Log(Level.Info, string.Format("Local observer listening to java bridge on: [{0}]", remoteManager.LocalEndpoint.ToString()));

            // Instantiate a remote observer to send messages to the java bridge.
            string[] javaAddressStrs = javaBridgeAddrStr.Split(':');
            IPAddress javaBridgeIpAddress = IPAddress.Parse(javaAddressStrs[0]);
            int port = int.Parse(javaAddressStrs[1]);
            IPEndPoint javaIpEndPoint = new IPEndPoint(javaBridgeIpAddress, port);
            remoteObserver = remoteManager.GetRemoteObserver(javaIpEndPoint);
            Logger.Log(Level.Info, string.Format("Connecting to java bridge on: [{0}]", javaIpEndPoint.ToString()));

            // Negotiate the protocol.
            Serializer.Initialize();
            remoteObserver.OnNext(Serializer.Write(new Protocol(100)));
        }
    }
}
