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
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Avro;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Bridge
{
    /// <summary>
    /// The CLR Bridge Network class agregates a RemoteManager and
    /// Protocol Serializer to provide a simple send/receive interface
    /// between the CLR and Java bridges. 
    /// </summary>
    public sealed class Network
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(Network));
        private IRemoteManager<byte[]> remoteManager;
        private IObserver<byte[]> remoteObserver;
        private LocalObserver localObserver;
        private BlockingCollection<byte[]> queue = new BlockingCollection<byte[]>();
        private ProtocolSerializer serializer;

        /// <summary>
        /// Construct a network stack using the wate remote manager.
        /// </summary>
        /// <param name="localAddressProvider">An address provider used to obtain a local IP address on an open port.</param>
        /// <param name="messageObserver">Message receiver that implements IObserver interface for each message to be processed</param>
        public Network(ILocalAddressProvider localAddressProvider, object messageObserver)
        {
            this.serializer = new ProtocolSerializer(typeof(Network).Assembly, "org.apache.reef.bridge.message");
            this.localObserver = new LocalObserver(serializer, messageObserver);

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

        /// <summary>
        /// Send a message to the java side of the bridge.
        /// </summary>
        /// <param name="identifier">A long value that which is the unique sequence identifier of the message.</param>
        /// <param name="message">An object reference to a message in the org.apache.reef.bridge.message package.</param>
        public void send(long identifier, object message)
        {
            Logger.Log(Level.Info, "Sending message: {0}", message);
            remoteObserver.OnNext(serializer.Write(message, identifier));
        }

        /// <summary>
        /// The Local Observer class receives byte buffer messages from the transport layer,
        /// deserializes the messages into Avro C# classes, and invokes the appropriate
        /// IObserver callback on the Avro message observer.
        /// </summary>
        private class LocalObserver : IObserver<IRemoteMessage<byte[]>>
        {
            private ProtocolSerializer serializer;
            private object messageObserver;

            public LocalObserver(ProtocolSerializer serializer, object messageObserver)
            {
                this.serializer = serializer;
                this.messageObserver = messageObserver;
            }

            /// <summary>
            /// Called by the remote manager to process messages received from the java bridge.
            /// </summary>
            /// <param name="message">A byte buffer containing a serialied message.</param>
            public void OnNext(IRemoteMessage<byte[]> message)
            {
                Logger.Log(Level.Info, "Message received: {0}", message.Identifier);

                // Deserialize the message and invoke the appropriate handler.
                serializer.Read(message.Message, messageObserver);
            }

            /// <summary>
            /// Handles error conditions in the low transport layer.
            /// </summary>
            /// <param name="error">The exception generated in the transport layer.</param>
            public void OnError(Exception error)
            {
                Logger.Log(Level.Info, "Error: [" + error.Message + "]");
            }

            /// <summary>
            /// Notification that no nore message prpocessing is required.
            /// </summary>
            public void OnCompleted()
            {
                Logger.Log(Level.Info, "Completed");
            }
        }

        /// <summary>
        /// Construct a remote manager to implement the low level network transport.
        /// </summary>
        /// <param name="localAddressProvider">An address provider used to obtain a local IP address on an open port.</param>
        /// <param name="javaBridgeAddress">A string which contains the IP and port of the java bridge.</param>
        private void BuildRemoteManager(
            ILocalAddressProvider localAddressProvider,
            string javaBridgeAddress)
        {
            // Instantiate the remote manager.
            IRemoteManagerFactory remoteManagerFactory =
                TangFactory.GetTang().NewInjector().GetInstance<IRemoteManagerFactory>();
            remoteManager = remoteManagerFactory.GetInstance(localAddressProvider.LocalAddress, new ByteCodec());

            // Listen to the java bridge on the local end point.
            remoteManager.RegisterObserver(localObserver);
            Logger.Log(Level.Info, string.Format("Local observer listening to java bridge on: [{0}]", remoteManager.LocalEndpoint.ToString()));

            // Instantiate a remote observer to send messages to the java bridge.
            string[] javaAddressStrs = javaBridgeAddress.Split(':');
            IPAddress javaBridgeIpAddress = IPAddress.Parse(javaAddressStrs[0]);
            int port = int.Parse(javaAddressStrs[1]);
            IPEndPoint javaIpEndPoint = new IPEndPoint(javaBridgeIpAddress, port);
            remoteObserver = remoteManager.GetRemoteObserver(javaIpEndPoint);
            Logger.Log(Level.Info, string.Format("Connecting to java bridge on: [{0}]", javaIpEndPoint.ToString()));

            // Negotiate the protocol.
            send(0, new BridgeProtocol(100));
        }
    }
}