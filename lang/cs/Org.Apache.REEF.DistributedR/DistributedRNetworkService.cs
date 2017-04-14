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
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.DistributedR
{
    internal class MessageHandler : IObserver<NsMessage<string>>
    {
        public BlockingCollection<NsMessage<string>> queue = new BlockingCollection<NsMessage<string>>();

        [Inject]
        public MessageHandler()
        {
        }

        public void OnNext(NsMessage<string> value)
        {
            queue.Add(value);
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

    public sealed class NetworkOptions
    {
        [NamedParameter(DefaultClass = typeof(INetworkService<string>), Documentation = "Network Service")]
        public class NetService : Name<INetworkService<string>>
        {
        }

        [NamedParameter(DefaultValue = "LocalServiceId", Documentation = "Service unique identifier")]
        public class ServiceIdentifier : Name<string>
        {
        }

        [NamedParameter(DefaultValue = "51511", Documentation = "Service connection port")]
        public class ServicePort : Name<int>
        {
        }

        [NamedParameter(DefaultValue = "RemoteServiceId", Documentation = "Remote service unique identifier")]
        public class RemoteServiceIdentifier : Name<string>
        {
        }

        [NamedParameter(DefaultClass = typeof(IObserver<NsMessage<string>>), Documentation = "Message handler implementation")]
        public class MessageHandler : Name<IObserver<NsMessage<string>>>
        {
        }

        public sealed class ModuleBuilder : ConfigurationModuleBuilder
        {
            public static readonly RequiredParameter<INetworkService<string>> NetService = new RequiredParameter<INetworkService<string>>();
            public static readonly RequiredParameter<string> ServiceIdentifier = new RequiredParameter<string>();
            public static readonly RequiredParameter<string> RemoteServiceIdentifier = new RequiredParameter<string>();
            public static readonly RequiredParameter<IObserver<NsMessage<string>>> MessageHandler = new RequiredParameter<IObserver<NsMessage<string>>>();
            public static readonly RequiredImpl<IObserver<NsMessage<string>>> MessageHandlerImpl = new RequiredImpl<IObserver<NsMessage<string>>>();
            public static readonly RequiredImpl<ICodec<string>> CodecImpl = new RequiredImpl<ICodec<string>>();

            public static readonly ConfigurationModule Config = new ModuleBuilder()
                .BindNamedParameter(GenericType<NetworkOptions.NetService>.Class, NetService)
                .BindNamedParameter(GenericType<NetworkOptions.ServiceIdentifier>.Class, ServiceIdentifier)
                .BindNamedParameter(GenericType<NetworkOptions.RemoteServiceIdentifier>.Class, RemoteServiceIdentifier)
                .BindNamedParameter(GenericType<NetworkOptions.MessageHandler>.Class, MessageHandler)
                .BindImplementation(GenericType<IObserver<NsMessage<string>>>.Class, MessageHandlerImpl)
                .BindImplementation(GenericType<ICodec<string>>.Class, CodecImpl)
                .Build();
        }
    }

    public class MessageService
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(MessageService));

        private INetworkService<string> networkService = null;
        private IObserver<NsMessage<string>> handler;
        private IConnection<string> connection;

        private CancellationTokenSource cancelSource;
        private CancellationToken cancelToken;
        private Thread listen;

        [Inject]
        private MessageService(
            [Parameter(typeof(NetworkOptions.NetService))] INetworkService<string> networkService,
            [Parameter(typeof(NetworkOptions.ServiceIdentifier))] string serviceIdentifier,
            [Parameter(typeof(NetworkOptions.RemoteServiceIdentifier))] string remoteServiceIdentifier,
            [Parameter(typeof(NetworkOptions.MessageHandler))] IObserver<NsMessage<string>> handler)
        {
            this.handler = handler;
            this.networkService = networkService;
            this.networkService.Register(new StringIdentifier(serviceIdentifier));
            this.connection = this.networkService.NewConnection(new StringIdentifier(remoteServiceIdentifier));
        }

        public void Start()
        {
            this.cancelSource = new CancellationTokenSource();
            this.cancelToken = this.cancelSource.Token;
            this.listen = new Thread(new ThreadStart(this.Listen));
            this.listen.Start();

            this.connection.Open();
        }

        public void Stop()
        {
            LOGGER.Log(Level.Info, "Request to stop listening");
            this.cancelSource.Cancel();
            this.listen.Join();
            this.cancelSource.Dispose();
        }

        private void Listen()
        {
            LOGGER.Log(Level.Info, "Starting to listen");
            var queue = ((MessageHandler)handler).queue;
            do
            {
                try
                {
                    var msg = queue.Take(this.cancelToken);
                    LOGGER.Log(Level.Info, "[" + msg.SourceId.ToString() + "->" + msg.DestId.ToString() + "] " + msg.Data[0]);
                }
                catch (Exception e)
                {
                    if (e is OperationCanceledException)
                    {
                        LOGGER.Log(Level.Info, "Listening canceled");
                    }
                }
            }
            while (!this.cancelToken.IsCancellationRequested);
            LOGGER.Log(Level.Info, "No longer listening");
        }

        public void Send(string script)
        {
            this.connection.Write(script);
        }
    }
}
