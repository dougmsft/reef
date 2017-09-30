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
using Org.Apache.REEF.Bridge;
using org.apache.reef.bridge.message;
using Org.Apache.REEF.Wake.Avro;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Tang.Annotations;
using System.Threading;

namespace Org.Apache.REEF.Driver.Bridge
{
    /// <summary>
    /// An Observer implementation which handles all of the messages defined in
    /// the Java to C# Avro protocol coming from the Java bridge which invokes
    /// the appropriate target method in the C# side of the driver.
    /// </summary>
    internal sealed class ClrBridge : IObserver<IMessageInstance<SystemOnStart>>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ClrBridge));
        private static long identifierSource = 0;
        private Network network;
        internal DriverBridge driverBridge { get; set; }

        /// <summary>
        /// Instantiate with 
        /// </summary>
        /// <param name="localAddressProvider">An address provider that
        /// will find an appropriate port for the CLR side of the bridge</param>
        [Inject]
        private ClrBridge(ILocalAddressProvider localAddressProvider)
        {
            this.network = new Network(localAddressProvider, this);
        }

        /// <summary>
        /// Callback to process the SystemOnStart message from the 
        /// Java side of the bridge.
        /// </summary>
        public void OnNext(IMessageInstance<SystemOnStart> systemOnStart)
        {
            Logger.Log(Level.Info, string.Format("*** SystemOnStart message received {0}", systemOnStart.Sequence));

            DateTime startTime = DateTime.Now;
            Logger.Log(Level.Info, "*** Start time is " + startTime);

            driverBridge.StartHandlersOnNext(startTime);
            long identifier = Interlocked.Increment(ref identifierSource);
            network.send(identifier, new Acknowledgement(systemOnStart.Sequence));
        }

        /// <summary>
        /// Handles error conditions in the bridge network.
        /// </summary>
        /// <param name="error">The exception generated in the transport layer.</param>
        public void OnError(Exception error)
        {
            Logger.Log(Level.Info, "JavaCLRBridge error: [" + error.Message + "]");
        }

        /// <summary>
        /// Notification that no nore message processing is required.
        /// </summary
        public void OnCompleted()
        {
            Logger.Log(Level.Info, "JavaCLRBridge OnCompleted");
        }
    }
}