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
using Org.Apache.REEF.Fluid.Message;
using Org.Apache.REEF.Fluid.Network;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Avro;

namespace Org.Apache.REEF.Fluid
{
    class TaskRunner
        : IObserver<IMessageInstance<RResultsMsg>>,
          IObserver<IMessageInstance<JuliaResultsMsg>>,
          IObserver<IMessageInstance<ShutdownMsg>>
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(TaskRunner));

        public BlockingCollection<object> toDriverQueue = new BlockingCollection<object>();
        public BlockingCollection<object> fromDriverQueue = new BlockingCollection<object>();

        private readonly MessageService network;

        private CancellationTokenSource cancelSource;
        private CancellationToken cancelToken;
        private Thread toDriverThread;

        internal TaskRunner()
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

        internal void Start()
        {
            Logr.Log(Level.Info, "Starting from client and from driver threads");
            this.network.Start();

            this.cancelSource = new CancellationTokenSource();
            this.cancelToken = this.cancelSource.Token;

            this.toDriverThread = new Thread(new ThreadStart(this.ProcessClientMessages));
            this.toDriverThread.Start();
        }

        internal void Stop()
        {
            Logr.Log(Level.Info, "Shutting down");

            Submit(new ShutdownMsg(0));
            this.cancelToken.WaitHandle.WaitOne(3000);

            if (this.cancelToken.IsCancellationRequested)
            {
                this.toDriverThread.Join();
            }
            else
            {
                this.cancelSource.Cancel();
            }

            this.network.Stop();
            this.cancelSource.Dispose();
        }

        public void Submit(object message)
        {
            this.toDriverQueue.Add(message, this.cancelToken);
        }

        public object GetResultsBlocking()
        {
            return this.fromDriverQueue.Take(this.cancelToken);
        }

        public bool TryGetResults(out object message)
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
                    object message = toDriverQueue.Take(this.cancelToken);
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

        public void OnNext(IMessageInstance<RResultsMsg> instance)
        {
            Logr.Log(Level.Info, "OnNext(RResultsMsg): " + instance.Message.ToString());
            fromDriverQueue.Add(instance.Message, this.cancelToken);
        }

        public void OnNext(IMessageInstance<JuliaResultsMsg> instance)
        {
            Logr.Log(Level.Info, "OnNext(JuliaResultsMsg): " + instance.Message.ToString());
            fromDriverQueue.Add(instance.Message, this.cancelToken);
        }

        public void OnNext(IMessageInstance<ShutdownMsg> instance)
        {
            Logr.Log(Level.Info, "OnNext(ShutdownMsg)");
            this.cancelSource.Cancel();
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
