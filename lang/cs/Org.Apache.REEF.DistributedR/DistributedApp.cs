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

using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace Org.Apache.REEF.DistributedR
{
    class DistributedApp
    {
        private static TaskRunner Runner { get; set; }

        /// <summary>
        /// Initializes the Distributed application
        /// </summary>
        /// <param name="workingDirectory">The working directory for assembly resolving</param>
        /// <param name="nodeCount">The number of nodes to use for running the application</param>
        public static void Initialize(string workingDirectory, int nodeCount)
        {
            RegisterAssemblyResolver(workingDirectory);

            // Configure and launch the job runner.
            Runner = new TaskRunner();

            // Configure and launch the driver.
            IConfiguration proxyConfigProviderConfig = DriverProxyConfigProviderOptions.ModuleBuilder.Config
                .Set(DriverProxyConfigProviderOptions.ModuleBuilder.RuntimeType, Runtime.Local.ToString())
                .Build();
            DriverProxyConfigurationProvider configProvider =
                TangFactory.GetTang().NewInjector(proxyConfigProviderConfig).GetInstance<DriverProxyConfigurationProvider>();
            DriverProxy driverProxy =
                TangFactory.GetTang().NewInjector(configProvider.GetConfiguration()).GetInstance<DriverProxy>();

            driverProxy.Run();

            // Start message processing.
            Runner.Start();

            //Initialize Job Scheduler
            JobScheduler.Initialize();
        }

        public static string SubmitJob(string function, List<string> dataList)
        {
            return JobScheduler.Instance.SubmitJob(function, dataList);
        }

        public static int QueryJobStatus(string jobId)
        {
            return JobScheduler.Instance.QueryJobStatus(jobId);
        }

        public static string GetJobResultsAsync(string jobId)
        {
            return JobScheduler.Instance.GetJobResultsAsync(jobId);
        }

        public static string GetJobResults(string jobId)
        {
            return JobScheduler.Instance.GetJobResults(jobId);
        }

        public static void Shutdown()
        {
            JobScheduler.Shutdown();
        }

        /// <summary>
        /// Registers an assembly resolver with the current application domain
        /// </summary>
        private static void RegisterAssemblyResolver(string workingDirectory)
        {
            s_workingDir = workingDirectory;
            AppDomain currentDomain = AppDomain.CurrentDomain;
            currentDomain.AssemblyResolve += new ResolveEventHandler(AssemblyResolveEventHandler);
        }

        /// <summary>
        /// Callback for when an assembly cant be resolved - tries to resolve the assembly using the specified working directory
        /// </summary>
        /// <param name="sender">The sender who fired the callback</param>
        /// <param name="args">The arguments of the resolve event</param>
        /// <returns>The resolved assembly</returns>
        protected static Assembly AssemblyResolveEventHandler(object sender, ResolveEventArgs args)
        {
            string name = new AssemblyName(args.Name).Name;
            var assemblyFile = System.IO.Path.Combine(s_workingDir, name);
            assemblyFile += ".dll";
            return Assembly.LoadFile(assemblyFile);
        }

        private static string s_workingDir;
    }
}
