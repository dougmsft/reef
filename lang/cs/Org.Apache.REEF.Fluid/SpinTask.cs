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
using System.Globalization;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Fluid
{
    /// <summary>
    /// A Task that merely prints a greeting and exits.
    /// </summary>
    public sealed class SpinTask : ITask
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(SpinTask));

        [Inject]
        private SpinTask()
        {
        }

        public void Dispose()
        {
            Console.WriteLine("Disposed.");
        }

        public byte[] Call(byte[] memento)
        {
            try
            {
                Thread.Sleep(3000);
            }
            catch (Exception except)
            {
                Logr.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Failed to execute R: [{0}] {1}", except, except.Message));
            }

            return ByteUtilities.StringToByteArrays("ReturnValue");
        }
    }
}