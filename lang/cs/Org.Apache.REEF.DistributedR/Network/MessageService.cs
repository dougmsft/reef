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
using System.Threading;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.DistributedR.Network
{
    public class MessageService : IObserver<object>
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(MessageService));

        private INetworkService<byte[]> networkService = null;
        private IObserver<NsMessage<byte[]>> handler;
        private IConnection<byte[]> connection;
        private string serviceIdentifier;
        private string remoteServiceIdentifier;

        private CancellationTokenSource cancelSource;
        private CancellationToken cancelToken;
        private Thread listen;

        public object Receiver { get; set; }

        [Inject]
        private MessageService(
            [Parameter(typeof(MessageServiceOptions.NetService))] INetworkService<byte[]> networkService,
            [Parameter(typeof(MessageServiceOptions.ServiceIdentifier))] string serviceIdentifier,
            [Parameter(typeof(MessageServiceOptions.RemoteServiceIdentifier))] string remoteServiceIdentifier,
            [Parameter(typeof(MessageServiceOptions.MessageHandler))] IObserver<NsMessage<byte[]>> handler)
        {
            this.handler = handler;
            this.networkService = networkService;
            this.serviceIdentifier = serviceIdentifier;
            this.remoteServiceIdentifier = remoteServiceIdentifier;
            this.Receiver = this; 

            Logr.Log(Level.Info, string.Format("Registering network service {0}", serviceIdentifier));
            this.networkService.Register(new StringIdentifier(serviceIdentifier));
        }

        public void Start()
        {
            Logr.Log(Level.Info, string.Format("Opening connection to remote service {0}", remoteServiceIdentifier));

            bool connected = false;
            while (!connected)
            {
                try
                {
                    this.connection = this.networkService.NewConnection(new StringIdentifier(remoteServiceIdentifier));
                    this.connection.Open();
                    connected = true;
                }
                catch (Exception e)
                {
                    Logr.Log(Level.Info, "Connect attempt failed." + e.ToString());
                    Thread.Sleep(250);
                }
            }

            this.cancelSource = new CancellationTokenSource();
            this.cancelToken = this.cancelSource.Token;
            this.listen = new Thread(new ThreadStart(this.Listen));
            this.listen.Start();
        }

        public void Stop()
        {
            Logr.Log(Level.Info, "Request to stop listening");
            this.cancelSource.Cancel();
            this.listen.Join();
            this.cancelSource.Dispose();
        }

        private void Listen()
        {
            Logr.Log(Level.Info, "Starting to listen");
            var queue = ((MessageHandler)handler).queue;
            while (!this.cancelToken.IsCancellationRequested)
            {
                try
                {
                    NsMessage<byte[]> msg = queue.Take(this.cancelToken);
                    Logr.Log(Level.Info, "Received message");
                    for (int idx = 0; idx < msg.Data.Count; ++idx)
                    {
                        Serializer.Read(msg.Data[idx], Receiver);
                    }
                }
                catch (Exception e)
                {
                    if (e is OperationCanceledException)
                    {
                        Logr.Log(Level.Info, "Listening canceled");
                    }
                }
            }
            Logr.Log(Level.Info, "No longer listening");
        }

        public void Send(IMessage message)
        {
            this.connection.Write(Serializer.Write(message));
        }

        public void OnNext(object obj)
        {
            Logr.Log(Level.Info, "OnNext(object): " + obj.ToString());          
        }
        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}
