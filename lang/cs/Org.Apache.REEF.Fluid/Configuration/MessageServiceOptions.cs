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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Fluid
{
    public sealed class MessageServiceOptions
    {
        [NamedParameter(DefaultClass = typeof(INetworkService<byte[]>), Documentation = "Network Service")]
        public class NetService : Name<INetworkService<byte[]>>
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

        [NamedParameter(DefaultClass = typeof(IObserver<NsMessage<byte[]>>), Documentation = "Message handler implementation")]
        public class MessageHandler : Name<IObserver<NsMessage<byte[]>>>
        {
        }

        public sealed class ModuleBuilder : ConfigurationModuleBuilder
        {
            public static readonly RequiredParameter<INetworkService<byte[]>> NetService = new RequiredParameter<INetworkService<byte[]>>();
            public static readonly RequiredParameter<string> ServiceIdentifier = new RequiredParameter<string>();
            public static readonly RequiredParameter<string> RemoteServiceIdentifier = new RequiredParameter<string>();
            public static readonly RequiredParameter<IObserver<NsMessage<byte[]>>> MessageHandler = new RequiredParameter<IObserver<NsMessage<byte[]>>>();
            public static readonly RequiredImpl<IObserver<NsMessage<byte[]>>> MessageHandlerImpl = new RequiredImpl<IObserver<NsMessage<byte[]>>>();
            public static readonly RequiredImpl<ICodec<byte[]>> CodecImpl = new RequiredImpl<ICodec<byte[]>>();

            public static readonly ConfigurationModule Config = new ModuleBuilder()
                .BindNamedParameter(GenericType<MessageServiceOptions.NetService>.Class, NetService)
                .BindNamedParameter(GenericType<MessageServiceOptions.ServiceIdentifier>.Class, ServiceIdentifier)
                .BindNamedParameter(GenericType<MessageServiceOptions.RemoteServiceIdentifier>.Class, RemoteServiceIdentifier)
                .BindNamedParameter(GenericType<MessageServiceOptions.MessageHandler>.Class, MessageHandler)
                .BindImplementation(GenericType<IObserver<NsMessage<byte[]>>>.Class, MessageHandlerImpl)
                .BindImplementation(GenericType<ICodec<byte[]>>.Class, CodecImpl)
                .Build();
        }
    }
}
