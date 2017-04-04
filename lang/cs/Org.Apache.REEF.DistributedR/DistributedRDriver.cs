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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.DistributedR
{
    public sealed class DriverOptions
    {
        [NamedParameter(Documentation = "Driver to client network")]
        public class Network : Name<MessageService>
        {
        }

        public sealed class ModuleBuilder : ConfigurationModuleBuilder
        {
            public static readonly RequiredParameter<MessageService> Network = new RequiredParameter<MessageService>();

            public static readonly ConfigurationModule Config = new ModuleBuilder()
                .BindNamedParameter(GenericType<DriverOptions.Network>.Class, Network)
                .Build();
        } 
    }

    /// <summary>
    /// The Driver for DistributedR: It requests a single Evaluator and then submits the HelloTask to it.
    /// </summary>
    public sealed class DistributedRDriver
        : IObserver<IDriverStarted>,
          IObserver<IAllocatedEvaluator>,
          IObserver<IRunningTask>,
          IObserver<ITaskMessage>,
          IObserver<ICompletedTask>
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(DistributedRDriver));

        private readonly IEvaluatorRequestor evaluatorRequestor;
        private readonly MessageService network;

        /// <summary>
        /// DistributedR task management logic.
        /// </summary>
        /// <param name="evaluatorRequestor"></param>
        [Inject]
        private DistributedRDriver(
            [Parameter(typeof(DriverOptions.Network))] MessageService network,
            IEvaluatorRequestor evaluatorRequestor)
        {
            this.network = network;
            this.evaluatorRequestor = evaluatorRequestor;
        }

        /// <summary>
        /// Called to start the user mode driver
        /// </summary>
        /// <param name="driverStarted"></param>
        public void OnNext(IDriverStarted driverStarted)
        {
            Logr.Log(Level.Info, string.Format("DistributedRDriver started at {0}", driverStarted.StartTime));
            this.evaluatorRequestor.Submit(this.evaluatorRequestor.NewBuilder().SetNumber(4).SetMegabytes(64).Build());
            this.network.Start();
        }

        /// <summary>
        /// Submits the DistributedRTask to the Evaluator.
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            IConfiguration taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "DistributedRTask")
                .Set(TaskConfiguration.Task, GenericType<DistributedRTask>.Class)
                .Build();

            IConfiguration distRTaskConfiguration = DistRTaskConfiguration.ConfigurationModule
                .Set(DistRTaskConfiguration.RScript, "writeMessage <- function() {\n print('TEST SCRIPT')\n }\n writeMessage()\n Sys.info()\n")
                .Build();

            Logr.Log(Level.Info, string.Format("DistR: EVALUATOR ALLOCATED"));
            allocatedEvaluator.SubmitTask(Configurations.Merge(taskConfiguration, distRTaskConfiguration));
        }
        
        public void OnNext(IRunningTask runningTask)
        {
            Logr.Log(Level.Info, "Received TaskRuntime: " + runningTask.Id);
        }

        public void OnNext(ITaskMessage taskMessage)
        {
            string msgReceived = ByteUtilities.ByteArraysToString(taskMessage.Message);
            Logr.Log(Level.Info, string.Format(msgReceived));
        }

        public void OnNext(ICompletedTask value)
        {
            Logr.Log(Level.Info, string.Format("DistR: TASK COMPLETED"));
            Logr.Log(Level.Info, ByteUtilities.ByteArraysToString(value.Message));
            value.ActiveContext.Dispose();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            Logr.Log(Level.Info, string.Format("DistR: COMPLETED"));
        }
    }
}