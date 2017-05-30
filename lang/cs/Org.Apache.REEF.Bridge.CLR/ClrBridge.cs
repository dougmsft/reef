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
using org.apache.reef.bridge.message;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Bridge
{
    /// <summary>
    /// Observer which handles all of the messages defined in the Java to C# Avro protocol
    /// and invokes the associated target method in the driver.
    /// </summary>
    public class ClrBridge :
        IObserver<SystemOnStart>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ClrBridge));
        private Network network;

        [Inject]
        private ClrBridge(ILocalAddressProvider localAddressProvider)
        {
            network = new Network(localAddressProvider, this);
        }

        public void OnNext(SystemOnStart systemOnStart)
        {
            Logger.Log(Level.Info, "SystemOnStart message: " + systemOnStart.dateTime);
        }

        public void OnError(Exception error)
        {
            Logger.Log(Level.Info, "JavaCLRBridge error: [" + error.Message + "]");
        }

        public void OnCompleted()
        {
            Logger.Log(Level.Info, "JavaCLRBridge OnCompleted");
        }
    }
}