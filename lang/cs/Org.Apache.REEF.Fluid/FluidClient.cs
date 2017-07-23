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
using System.Net.Sockets;
using Org.Apache.REEF.Utilities.Logging;
using System.Threading;
using System.Collections.Concurrent;

namespace Org.Apache.REEF.Fluid
{
    class FluidClient
    {
        private static readonly Logger LOG = Logger.GetLogger(typeof(FluidClient));

        private BlockingCollection<byte[]> messages;
        private CancellationTokenSource cancelSource;
        private CancellationToken cancelToken; 

        private Thread reader;
        private Thread writer;

        private TcpClient client;
        private NetworkStream stream;
        private byte[] buffer = new byte[1048576];

        private IObserver<byte[]> observer;

        public Guid UUID { get; }

        public bool IsActive()
        {
            return cancelSource != null;
        }

        public FluidClient(TcpClient client, IObserver<byte[]> observer)
        {
            UUID = Guid.NewGuid();

            this.client = client;
            this.observer = observer;

            stream = client.GetStream();
            buffer = new byte[1048576];
            messages = new BlockingCollection<byte[]>(new ConcurrentQueue<byte[]>());
        }

        public void Start()
        {
            lock (this)
            {
                if (cancelSource == null)
                {
                    cancelSource = new CancellationTokenSource();
                    cancelToken = cancelSource.Token;

                    reader = new Thread(new ThreadStart(this.Read));
                    writer = new Thread(new ThreadStart(this.Write));

                    writer.Start();
                    reader.Start();
                }
                else
                {
                    LOG.Log(Level.Error, "Start called on running Fluid client.");
                }
            }
        }

        public void Stop()
        {
            lock (this)
            {
                if (cancelSource != null)
                {
                    cancelSource.Cancel();
                    stream.Dispose();

                    reader.Join();
                    writer.Join();

                    cancelSource.Dispose();
                    cancelSource = null;
                }
                else
                {
                    LOG.Log(Level.Error, "Stop called on non running Fluid client.");
                }
            }
        }

        private void Read()
        {
            try
            {
                int size;
                do
                {
                    size = stream.Read(buffer, 0, buffer.Length);
                    observer.OnNext(buffer);
                }
                while (!cancelToken.IsCancellationRequested);
            }
            catch (Exception e)
            {
                LOG.Log(Level.Info, "Fluid client reader exception.", e);
            }
            finally
            {
                if (!Monitor.IsEntered(this))
                {
                    Stop();
                }
            }
        }

        private void Write()
        {
            try
            {
                do
                {
                    byte[] buffer = messages.Take(cancelToken);
                    if (!cancelToken.IsCancellationRequested)
                    {
                        stream.Write(buffer, 0, buffer.Length);
                    }
                }
                while (!cancelToken.IsCancellationRequested);
            }
            catch (Exception e)
            {
                if (e is OperationCanceledException)
                {
                    LOG.Log(Level.Info, "Fluid client writer exiting.");
                }
                else
                {
                    LOG.Log(Level.Error, "Fluid client write exeception.", e);
                }
            }
            finally
            {
                if (!Monitor.IsEntered(this))
                {
                    Stop();
                }
            }
        }
    }
}
