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
using Grpc.Core;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Bridge.Proto;

namespace Org.Apache.REEF.Bridge.Client
{
    public class DriverServiceClient : IDriverServiceClient
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DriverServiceClient));
        private readonly DriverService.DriverServiceClient serviceStub;

        public DriverServiceClient(Int32 driverServicePort)
        {
            Channel channel = new Channel("localhost", driverServicePort, ChannelCredentials.Insecure);
            serviceStub = new DriverService.DriverServiceClient(channel);
        }

        public void RegisterDriverClientsService(string host, int port)
        {
            DriverClientRegistration registration = new DriverClientRegistration
            {
                Host = host,
                Port = port
            };
            serviceStub.RegisterDriverClient(registration);
        }

        public void OnShutdown()
        {
            ShutdownRequest request = new ShutdownRequest();
            serviceStub.Shutdown(request);
        }

        public void OnShutdown(Exception ex)
        {
            ExceptionInfo info = new ExceptionInfo
            {
                Name = ex.GetType().Name,
                Message = ex.Message,
            };
            info.StackTrace.Add(ex.StackTrace);

            ShutdownRequest request = new ShutdownRequest
            {
                Exception = info
            };

            serviceStub.Shutdown(request);
        }

        public void OnSetAlarm()
        {

        }
    }
}