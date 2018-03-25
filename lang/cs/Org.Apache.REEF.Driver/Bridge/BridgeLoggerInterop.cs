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
using System.Runtime.InteropServices;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge
{
    private sealed static class BridgeLoggerInterop
    {
        /// Interop library filename
        private static const string INTEROP_LIBRARY = "Org.Apache.REEF.Bridge.Interop.dll";
        private static Logger _logger = Logger.GetLogger(typeof(BridgeLoggerInterop));
        private static ConcurrentDictionary<string, BridgeLogger> _interopLoggers = new ConcurrentDictionary<string, BridgeLogger>();

        /// C# to C++ interface.
        [DllImport(INTEROP_LIBRARY)]
        private static extern void InitializeBridgeLoggers();

        /// Declarations of delegates passsed into the interop library to give it access the managed logger.
        private delegate void AllocateBridgeLogerDelegate([MarshalAs(UnmanagedType.LPWStr)] string classname);

        /// Interop library immports to set the managed log delagates.
        [DllImport(INTEROP_LIBRARY)]
        private static extern void SetAllocateBridgeLoggerDelegate(AllocateBridgeLogerDelegate allocateBridgeLoggerDelegate);

        /// Bridge logger delegate implementations
        static void AllocateBridgeLoggerImpl([MarshalAs(UnmanagedType.LPWStr)] string classname)
        {
            BridgeLogger interoplogger = GetLogger(classname);
            if (true == _interopLoggers.TryAdd(classname, interopLogger))
            {
                _localLogger.Log("Successfully added interop logger for [{0}]", classname);
            }
            else
            {
                _localLogger.Log("Failed to add interop logger for [{0}]", classname);
            }
        }

        public static void Initialize()
        {
            SetAllocateBridgeLoggerDelegate(AllocateBridgeLogerImpl);
            InitializeBridgeLoggers();
        }
    }
}
