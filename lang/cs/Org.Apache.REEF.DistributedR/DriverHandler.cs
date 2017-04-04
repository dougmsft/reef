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
using System.Globalization;
using Org.Apache.REEF.DistributedR.Network;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
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
    public sealed class DriverHandler
        : IObserver<IDriverStarted>,
          IObserver<IAllocatedEvaluator>,
          IObserver<IActiveContext>, 
          IObserver<IRunningTask>,
          IObserver<ITaskMessage>,
          IObserver<ICompletedTask>,
          IObserver<RTaskMsg>
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(DriverHandler));
        private const string spinTaskId = "SpinTask";

        private readonly IEvaluatorRequestor evaluatorRequestor;
        private readonly MessageService network;

        private ConcurrentQueue<RTaskMsg> waitingTasks = new ConcurrentQueue<RTaskMsg>();
        private ConcurrentDictionary<string, RTaskMsg> runningTasks = new ConcurrentDictionary<string, RTaskMsg>();

        /// <summary>
        /// DistributedR task management logic.
        /// </summary>
        /// <param name="evaluatorRequestor"></param>
        [Inject]
        private DriverHandler(
            [Parameter(typeof(DriverOptions.Network))] MessageService network,
            IEvaluatorRequestor evaluatorRequestor)
        {
            this.network = network;
            this.network.Receiver = this;
            this.evaluatorRequestor = evaluatorRequestor;
        }

        private IConfiguration GetSpinTaskConfiguration()
        {
            return TaskConfiguration.ConfigurationModule
               .Set(TaskConfiguration.Identifier, spinTaskId)
               .Set(TaskConfiguration.Task, GenericType<SpinTask>.Class)
               .Build();
        }

        private IConfiguration GetTaskConfiguration(string id, string function, string data)
        {
            IConfiguration taskConfiguration = TaskConfiguration.ConfigurationModule
               .Set(TaskConfiguration.Identifier, id)
               .Set(TaskConfiguration.Task, GenericType<RExecTask>.Class)
               .Build();

            IConfiguration distRTaskConfiguration = RExecTaskConfiguration.ConfigurationModule
                .Set(RExecTaskConfiguration.Function, function)
                .Set(RExecTaskConfiguration.Data, data)
                .Build();

            return Configurations.Merge(taskConfiguration, distRTaskConfiguration);
        }

        /// <summary>
        /// Called to start the user mode driver
        /// </summary>
        /// <param name="driverStarted"></param>
        public void OnNext(IDriverStarted driverStarted)
        {
            Logr.Log(Level.Info, string.Format("DriverHandlers started at {0}", driverStarted.StartTime));
            this.evaluatorRequestor.Submit(this.evaluatorRequestor.NewBuilder().SetNumber(4).SetMegabytes(64).Build());
            this.network.Start();
        }

        /// <summary>
        /// Submits the RExecTask to the Evaluator.
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            Logr.Log(Level.Info, string.Format("EVALUATOR ALLOCATED"));

            // Run one of the waiting tasks.
            RTaskMsg taskMsg;
            if (this.waitingTasks.TryDequeue(out taskMsg))
            {
                Logr.Log(Level.Info, string.Format("SUBMITTING NEW TASK"));
                string guidstr = taskMsg.Id.ToString();
                runningTasks.TryAdd(taskMsg.Id.ToString(), taskMsg);
                allocatedEvaluator.SubmitTask(GetTaskConfiguration(guidstr, taskMsg.Function, taskMsg.Data));
            }
            else
            {
                // Hold the evaluator with a spin task.
                Logr.Log(Level.Info, "Submitting Spin Task");
                allocatedEvaluator.SubmitTask(GetSpinTaskConfiguration());
            }
        }

        public void OnNext(IActiveContext activeContext)
        {
            Logr.Log(Level.Info, string.Format("EVALUATOR ACTIVE"));

            /// activeContext.SubmitTask(GetTaskConfiguration());
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
            Logr.Log(Level.Info, string.Format("TASK COMPLETED" + value.Id));
            Logr.Log(Level.Info, ByteUtilities.ByteArraysToString(value.Message));

            RTaskMsg taskMsg;
            string guidstr = value.Id;
            if (!guidstr.ToLower().Contains(spinTaskId.ToLower()))
            {
                // Process the return value of the task.
                if (runningTasks.TryRemove(guidstr, out taskMsg))
                {
                    RResultsMsg resultsMsg = RResultsMsg.Factory(ByteUtilities.ByteArraysToString(value.Message));
                    network.Send(resultsMsg);
                }
                else
                {
                    Logr.Log(Level.Error, string.Format("Unable to find meta data for completed task: " + value.Id));
                }
            }

            if (this.waitingTasks.TryDequeue(out taskMsg))
            {
                Logr.Log(Level.Info, string.Format("SUBMITTING NEW TASK"));
                guidstr = taskMsg.Id.ToString();
                runningTasks.TryAdd(taskMsg.Id.ToString(), taskMsg);
                value.ActiveContext.SubmitTask(GetTaskConfiguration(guidstr, taskMsg.Function, taskMsg.Data));
            } 
            else
            {
                // Hold the evaluator with a spin task.
                Logr.Log(Level.Info, "Submitting Spin Task");
                value.ActiveContext.SubmitTask(GetSpinTaskConfiguration());
            }

            /// value.ActiveContext.Dispose();
        }
        public void OnNext(RTaskMsg rTask)
        {
            Logr.Log(Level.Info, "OnNext(RTaskMsg): " + rTask.ToString());
            waitingTasks.Enqueue(rTask);
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            Logr.Log(Level.Info, "COMPLETED");
        }
    }
}