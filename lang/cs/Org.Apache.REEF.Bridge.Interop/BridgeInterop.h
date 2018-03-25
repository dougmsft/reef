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

#ifndef BRIDGE_INTEROP_H
#define BRIDGE_INTEROP_H

#ifdef _WINDOWS
    #define BRIDGE_INTEROP_API __declspec(dllexport)   
#else  
    #define BRIDGE_INTEROP_API 
#endif  

#ifdef _WINDOWS
    #include <SDKDDKVer.h>
    #define WIN32_LEAN_AND_MEAN 
    #include <windows.h>
#endif


/// Delegate function pointer type definitions.
typedef void(*AllocateBridgeLoggerDelegatePtr)(wchar_t const* classname);
    
extern  "C"
{
    BRIDGE_INTEROP_API void InitializeBridgeLoggers();

    /// Delegates set from C#.
    BRIDGE_INTEROP_API void SetAllocateBridgeLoggerDelagate(AllocateBridgeLoggerDelegatePtr allocateBridgeLoggerDelegate);
}

#endif // BRIDGE_INTEROP_H
