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
using System.Collections.Concurrent;
using Org.Apache.REEF.DistributedR.Network;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.DistributedR
{
    class TaskRunner : IObserver<RResultsMsg>
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(TaskRunner));

        public BlockingCollection<IMessage> toDriverQueue = new BlockingCollection<IMessage>();
        public BlockingCollection<IMessage> fromDriverQueue = new BlockingCollection<IMessage>();

        private readonly MessageService network;

        private CancellationTokenSource cancelSource;
        private CancellationToken cancelToken;
        private Thread toDriverThread;

        public TaskRunner()
        {
            // Instantiate the network service.
            IConfiguration netConfigProviderCofig = NetConfigProviderOptions.ModuleBuilder.Config
                .Set(NetConfigProviderOptions.ModuleBuilder.NetType, NetworkType.Client.ToString())
                .Build();
            NetworkConfigurationProvider configProvider =
                TangFactory.GetTang().NewInjector(netConfigProviderCofig).GetInstance<NetworkConfigurationProvider>();

            this.network = TangFactory.GetTang().NewInjector(configProvider.GetConfiguration()).GetInstance<MessageService>();
            this.network.Receiver = this;
        }

        public void Start()
        {
            Logr.Log(Level.Info, "Starting from client and from driver threads");
            this.network.Start();

            this.cancelSource = new CancellationTokenSource();
            this.cancelToken = this.cancelSource.Token;

            this.toDriverThread = new Thread(new ThreadStart(this.ProcessClientMessages));
            this.toDriverThread.Start();
        }

        public void Stop(bool wait = false)
        {
            Logr.Log(Level.Info, "Shutting down");

            if (!wait)
            {
                this.cancelSource.Cancel();
            }
            this.toDriverThread.Join();
            this.cancelSource.Dispose();

            this.network.Stop();
        }

        public void Submit(IMessage message)
        {
            this.toDriverQueue.Add(message, this.cancelToken);
        }

        public IMessage GetResultsBlocking()
        {
            return this.fromDriverQueue.Take(this.cancelToken);
        }

        public bool TryGetResults(out IMessage message)
        {
            return this.fromDriverQueue.TryTake(out message);
        }

        private void ProcessClientMessages()
        {
            Logr.Log(Level.Info, "Listening for client task submissions");
            do
            {
                try
                {
                    IMessage message = toDriverQueue.Take(this.cancelToken);
                    if (!this.cancelToken.IsCancellationRequested)
                    {
                        network.Send(message);
                        Logr.Log(Level.Info, string.Format("Sending task id: {0} to driver", message.ToString()));
                    }
                }
                catch (Exception e)
                {
                    if (e is OperationCanceledException)
                    {
                        Logr.Log(Level.Info, "Listening for client task submissions canceled");
                    }
                }
            }
            while (!this.cancelToken.IsCancellationRequested);
        }

        public void OnNext(RResultsMsg results)
        {
            Logr.Log(Level.Info, "OnNext(RTaskMsg): " + results.ToString());
            fromDriverQueue.Add(results, this.cancelToken);
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
