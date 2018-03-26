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

#include "BridgeInterop.h"
#include "BridgeInteropLogger.h"
#include <string>

/// Namespace usage
using namespace std;

/// <summary>
/// Anonymous namespace to hold the pointers to C# delegates needed to call into C#.
/// </summary>
namespace
{
    /// Delegate function pointer type definitions.
    typedef int32_t (*AllocateBridgeLoggerDelegatePtr)(wchar_t const* classname);
    typedef void (*LogDelegatePtr)(int32_t index, wchar_t const* message);

    AllocateBridgeLoggerDelegatePtr     _allocateBridgeLoggerDelegate = 0;
    LogDelegatePtr                      _logDelegate = 0;
}

extern  "C"
{
    BRIDGE_INTEROP_API void SetAllocateBridgeLoggerDelegate(AllocateBridgeLoggerDelegatePtr allocateBridgeLoggerDelegate)
    {
        _allocateBridgeLoggerDelegate = allocateBridgeLoggerDelegate;
    }

    BRIDGE_INTEROP_API void SetLogDelegate(LogDelegatePtr logDelegate)
    {
        _logDelegate= logDelegate;
    }
}

///
///
Org::Apache::REEF::Driver::Bridge::BridgeInteropLogger::BridgeInteropLogger(wstring classname)
{
    _index = _allocateBridgeLoggerDelegate(classname.c_str());
}

void Org::Apache::REEF::Driver::Bridge::BridgeInteropLogger::Log(wstring message)
{
    _logDelegate(_index, message.c_str());
}
