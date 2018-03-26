// Licensed to the Apache Software Founda;tion (ASF) under one
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
using System.Threading;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge
{
    [Private]
    public static class BridgeInteropLogger
    {
        /// Interop library filename
        private const string INTEROP_LIBRARY = "Org.Apache.REEF.Bridge.Interop.dll";
        private static readonly Logger _logger = Logger.GetLogger(typeof(BridgeInteropLogger));
        private static ConcurrentDictionary<Int32, BridgeLogger> _interopLoggers = new ConcurrentDictionary<Int32, BridgeLogger>();
        private static Int32 _index = 0;

        /// C# to C++ interface.
        [DllImport(INTEROP_LIBRARY)]
        private static extern void TestBridgeLoggers();

        /// Declarations of delegates passsed into the interop library to give it access the managed logger.
        private delegate Int32 AllocateBridgeLogerDelegate([MarshalAs(UnmanagedType.LPWStr)] string classname);
        private delegate void LogDelegate(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message);

        /// Interop library immports to set the managed log delagates.
        [DllImport(INTEROP_LIBRARY)]
        private static extern void SetAllocateBridgeLoggerDelegate(AllocateBridgeLogerDelegate allocateBridgeLoggerDelegate);
        [DllImport(INTEROP_LIBRARY)]
        private static extern void SetLogDelegate(LogDelegate logDelegate);

        /// Bridge logger delegate implementations
        static Int32 AllocateBridgeLoggerImpl([MarshalAs(UnmanagedType.LPWStr)] string classname)
        {
            BridgeLogger interopLogger = BridgeLogger.GetLogger(classname);
            Int32 index = Interlocked.Increment(ref _index);
            if (true == _interopLoggers.TryAdd(index, interopLogger))
            {
                _logger.Log(Level.Info, "Successfully added interop logger for [{0}] with index [{1}]", classname, index);
            }
            else
            {
                _logger.Log(Level.Error, "Failed to add interop logger for [{0}]", classname);
            }
            return index;
        }

        static void LogDelegateImpl(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message)
        {
            BridgeLogger interopLogger;
            if (_interopLoggers.TryGetValue(index, out interopLogger))
            {
                interopLogger.Log(message);
            }
            else
            {
                _logger.Log(Level.Error, "Invalid logger requested for id = [{0}]", index);
            }
        }

        public static void Initialize()
        {
            SetAllocateBridgeLoggerDelegate(AllocateBridgeLoggerImpl);
            SetLogDelegate(LogDelegateImpl);
            TestBridgeLoggers();
        }
    }
}
