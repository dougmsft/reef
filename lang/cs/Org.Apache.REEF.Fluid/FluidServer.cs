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
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Fluid
{
    /// <summary>
    /// Listens for incomming client connections.
    /// </summary>
    class FluidServer
    {
        private static readonly Logger LOG = Logger.GetLogger(typeof(FluidServer));

        private Thread listenerThread;
        private long listening = 0;

        private TcpListener listener;
        private ConcurrentDictionary<EndPoint, FluidClient> clients;

        public int Port { get;  }
        public IPAddress Address { get;  }

        /// <summary>
        /// Initialize a server on the specified port and IP address.
        /// </summary>
        /// <param name="port">Port on which the server will listen.</param>
        /// <param name="address">Internet protocol address of the server.</param>
        public FluidServer(int port = 51515, string address = "127.0.0.1")
        {
            Port = port;
            Address = IPAddress.Parse(address);
            clients = new ConcurrentDictionary<EndPoint, FluidClient>();
        }

        /// <summary>
        /// Start the server listening for client connections.
        /// </summary>
        public void Start()
        {
            if (Interlocked.Read(ref listening) == 0)
            {
                LOG.Log(Level.Info, "Fluid Server starting.");

                Interlocked.Increment(ref listening);
                listenerThread = new Thread(new ThreadStart(this.Listen));
                listener.Start();
            }
            else
            {
                LOG.Log(Level.Error, "Attempt to call Start on running Fluid server.");
            }
        }

        /// <summary>
        /// Stop the server from listening for client connections.
        /// </summary>
        public void Stop()
        {
            if (Interlocked.Read(ref listening) == 1)
            {
                LOG.Log(Level.Info, "Fluid Server stopping.");

                Interlocked.Decrement(ref listening);
                listener.Stop();

                foreach (var entry in clients)
                {
                    entry.Value.Stop();
                }
            }
        }

        private void Listen()
        {
            try
            {
                listener = new TcpListener(Address, Port);
                listener.Start();

                while (Interlocked.Read(ref listening) != 0)
                {
                    LOG.Log(Level.Info, "Fluid Server waiting for connection.");
                    TcpClient tcpClient = listener.AcceptTcpClient();
                    LOG.Log(Level.Info, "Fluid Server accepting connection: {0}", tcpClient.Client.RemoteEndPoint.ToString());

                    FluidClient client = new FluidClient(tcpClient, new ClientMessageHandler());
                    if (!clients.TryAdd(tcpClient.Client.RemoteEndPoint, client))
                    {
                        LOG.Log(Level.Error, "Attempt to add existing remote client.");
                    }
                    client.Start();
                }
            }
            catch (Exception e)
            {
                LOG.Log(Level.Info, "Socket error: ", e);
            }
        }
    }
}
