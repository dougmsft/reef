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
    internal static class BridgeLoggerDelegates
    {
        /// Declarations of delegates passsed into the interop library to give it access the managed logger.
        public delegate Int32 AllocateLogger([MarshalAs(UnmanagedType.LPWStr)] string classname);
        public delegate void Log(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message);
        public delegate void LogStart(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message);
        public delegate void LogStop(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message);
        public delegate void LogError(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message,  [MarshalAs(UnmanagedType.LPWStr)] string execp);
    }

    internal static class BridgeLoggerLibrary
    {
        /// Interop library filename
        private const string INTEROP_LIBRARY = "Org.Apache.REEF.Bridge.Interop.dll";

        /// Interop library immports to set the bridge interop logger delegates in the C++ library.
        [DllImport(INTEROP_LIBRARY)]
        public static extern void SetAllocateBridgeLoggerDelegate(BridgeLoggerDelegates.AllocateLogger allocateLogger);

        [DllImport(INTEROP_LIBRARY)]
        public static extern void SetLogDelegate(BridgeLoggerDelegates.Log log);

        [DllImport(INTEROP_LIBRARY)]
        public static extern void SetLogStartDelegate(BridgeLoggerDelegates.LogStart logStart);

        [DllImport(INTEROP_LIBRARY)]
        public static extern void SetLogStopDelegate(BridgeLoggerDelegates.LogStop logStop);

        [DllImport(INTEROP_LIBRARY)]
        public static extern void SetLogErrorDelegate(BridgeLoggerDelegates.LogError logError);

        [DllImport(INTEROP_LIBRARY)]
        public static extern void TestBridgeLoggers();
    }

    public static class BridgeInteropLogger
    {
        /// Local logger
        private static readonly Logger _logger = Logger.GetLogger(typeof(BridgeInteropLogger));
        /// Dictionary fo=r tracking instances.
        private static ConcurrentDictionary<Int32, BridgeLogger> _interopLoggers = new ConcurrentDictionary<Int32, BridgeLogger>();
        private static Int32 _index = 0;

        /// Pinned delegates
        private static GCHandle allocateLogger;
        private static GCHandle log;
        private static GCHandle logStart;
        private static GCHandle logStop;
        private static GCHandle logError;

        public static void Initialize()
        {
            InitializeDelegates();

            BridgeLoggerLibrary.SetAllocateBridgeLoggerDelegate((BridgeLoggerDelegates.AllocateLogger)allocateLogger.Target);
            BridgeLoggerLibrary.SetLogDelegate((BridgeLoggerDelegates.Log)log.Target);
            BridgeLoggerLibrary.SetLogStartDelegate((BridgeLoggerDelegates.LogStart)logStart.Target);
            BridgeLoggerLibrary.SetLogStopDelegate((BridgeLoggerDelegates.LogStop)logStop.Target);
            BridgeLoggerLibrary.SetLogErrorDelegate((BridgeLoggerDelegates.LogError)logError.Target);
            BridgeLoggerLibrary.TestBridgeLoggers();
        }

        /// Bridge logger delegate implementations
        private static Int32 AllocateBridgeLoggerImpl([MarshalAs(UnmanagedType.LPWStr)] string classname)
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

        private static void LogImpl(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message)
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

        private static void LogStartImpl(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message)
        {
            BridgeLogger interopLogger;
            if (_interopLoggers.TryGetValue(index, out interopLogger))
            {
                interopLogger.LogStart(message);
            }
            else
            {
                _logger.Log(Level.Error, "Invalid logger requested for id = [{0}]", index);
            }
        }

        private static void LogStopImpl(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message)
        {
            BridgeLogger interopLogger;
            if (_interopLoggers.TryGetValue(index, out interopLogger))
            {
                interopLogger.LogStop(message);
            }
            else
            {
                _logger.Log(Level.Error, "Invalid logger requested for id = [{0}]", index);
            }
        }

        private static void LogErrorImpl(Int32 index, [MarshalAs(UnmanagedType.LPWStr)] string message,  [MarshalAs(UnmanagedType.LPWStr)] string excep)
        {
            BridgeLogger interopLogger;
            if (_interopLoggers.TryGetValue(index, out interopLogger))
            {
                interopLogger.LogError(message, new Exception(excep));
            }
            else
            {
                _logger.Log(Level.Error, "Invalid logger requested for id = [{0}]", index);
            }
        }

        private static void InitializeDelegates()
        {
            allocateLogger = GCHandle.Alloc(new BridgeLoggerDelegates.AllocateLogger(AllocateBridgeLoggerImpl), GCHandleType.Pinned);
            log = GCHandle.Alloc(new BridgeLoggerDelegates.Log(LogImpl), GCHandleType.Pinned);
            logStart = GCHandle.Alloc(new BridgeLoggerDelegates.LogStart(LogStartImpl), GCHandleType.Pinned);
            logStop = GCHandle.Alloc(new BridgeLoggerDelegates.LogStop(LogStopImpl), GCHandleType.Pinned);
            logError = GCHandle.Alloc(new BridgeLoggerDelegates.LogError(LogErrorImpl), GCHandleType.Pinned);
        }

        private static void UninitializeDelegates()
        {
            allocateLogger.Free();
            log.Free();
            logStart.Free();
            logStop.Free();
            logError.Free();
        }
    }
}
