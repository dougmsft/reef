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
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.DistributedR.Network;

namespace Org.Apache.REEF.DistributedR
{
    /// <summary>
    /// The Controller can be called by an controlling process (such as an R application)
    /// and provides the functionality of controlling the cluster size to run the job, 
    /// submit a job, check the status of a job and shutdown the cluster. This class exposes
    /// static functions allowing them to be invoked outside of the library.
    /// </summary>
    public class JobScheduler 
    {

        public static JobScheduler Instance
        {
            get;
            private set;
        }

        public string WorkingDirectory
        {
            get;
            private set;
        }

        public int NodeCount
        {
            get;
            private set;
        }

        private Dictionary<string, string> JobIdToJobMap
        {
            get;
            set;
        }

        private Dictionary<string, string> TaskIdToJobIdMap
        {
            get;
            set;
        }

        /// <summary>
        /// Initializes the job scheduler
        /// </summary>
        public static void Initialize()
        {
            if (Instance!= null)
            {
                throw new InvalidOperationException("JobScheduler instance already exists.");
            }

            Instance = new JobScheduler();
        }

        public static void Shutdown()
        {
            if (Instance != null)
            {
                Instance.ShutdownInternal();
                Instance = null;
            }
        }

        private JobScheduler()
        {
            JobIdToJobMap = new Dictionary<string, string>();
            TaskIdToJobIdMap = new Dictionary<string, string>();
        }

        public string SubmitJob(string function, IList<string> dataList)
        {
            Job job = new Job();

            // TODO: Get data from dispatcher
            foreach (string data in dataList)
            {
                // Construct the task
                var task = RTaskMsg.Factory(function, data);
                job.AddTaskId(task.Id);

                // Schedule task

            }


            // Create a task for each item that is passed
            _jobQueue.Enqueue(job);
            job.State = JobState.Queued;
            return job.Id;
        }

        /// <summary>
        /// Internal function to check if a job is complete
        /// </summary>
        /// <param name="jobId">The job id</param>
        /// <returns>True if job is done, false otherwise</returns>
        public int QueryJobStatus(string jobId)
        {
            
        }

        /// <summary>
        /// Internal function to retrieve the results of a job
        /// </summary>
        /// <param name="jobId">The job id</param>
        /// <returns>Returns the results if job is found, otherwise an empty data set.</returns>
        public string GetJobResultsAsync(string jobId)
        {
            var job = GetJobById(jobId);
            if (job == null)
            {
                return string.Empty;
            }

            return job.Results;
        }

        /// <summary>
        /// Internal function to retrieve the results of a job
        /// </summary>
        /// <param name="jobId">The job id</param>
        /// <returns>Returns the results if job is found, otherwise an empty data set.</returns>
        public string GetJobResults(string jobId)
        {
            var job = GetJobById(jobId);
            if (job == null)
            {
                return string.Empty;
            }

            return job.Results;
        }

        /// <summary>
        /// Internal shutdown function
        /// </summary>
        private void ShutdownInternal()
        {
        }

        /*
        /// <summary>
        /// Start receiving data from the calling application.
        /// </summary>
        private void StartReceivingData()
        {
            /// Data is sent from the calling application to C# over a socket.
            /// This initializes the data transport layer to start listening on the socket for any incoming data.
            _dataTransport = new DataTransport();
            _dataTransport.StartReceiveService(11000);
        }

        /// <summary>
        /// Stops listening for data from the calling application.
        /// </summary>
        private void StopReceivingData()
        {
            /// Stop listening for data.
            _dataTransport.Stop();
        }
        */

        private static JobScheduler _instance;
    }
}
