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

#include <windows.h>
#include <vector>
#include "FluidCoreInterface.h"

static HINSTANCE dllHandle;
#define MAX_ID_LENGTH  128

// Fluid library function signatures.
typedef void (*ConnectFunc)(const char* ipAddress, int port);
typedef void (*DisconnectFunc)();
typedef const char* (*SubmitRTaskFunc)(const unsigned char* byteArray, int size, char* id, int idLength);
typedef const char* (*SubmitJuliaTaskFunc)(const unsigned char* byteArray, int size, char* id, int idLength);

// Fluid library function pointers.
static ConnectFunc FCConnect = nullptr;
static DisconnectFunc FCDisconnect = nullptr;
static SubmitRTaskFunc FCSubmitRTask = nullptr;
static SubmitJuliaTaskFunc FCSubmitJuliaTask = nullptr;

//! \brief Loads the core Fluid library
//! \detailed The fluidclient library is loaded at runtime. This will attempt to load the library given the
//!    working directory and find the functions within the library.
//! \param fluidLibDir The directory where the Fluid core library binary is located.
//! \return 0 if successful, non-zero if not successful.
int fluid::Initialize(std::string const& fluidLibDir)
{
    std::string fullPath = fluidLibDir + "/fluid_core_client.dll";
    dllHandle = LoadLibrary(fullPath.c_str());
    if (dllHandle == NULL)
    {
        return (int)GetLastError();
    }

    FCConnect = (ConnectFunc)GetProcAddress(dllHandle, "Connect");
    FCDisconnect = (DisconnectFunc)GetProcAddress(dllHandle, "Disconnect");
    FCSubmitRTask = (SubmitRTaskFunc)GetProcAddress(dllHandle, "SubmitRTask");
    FCSubmitJuliaTask = (SubmitJuliaTaskFunc)GetProcAddress(dllHandle, "SubmitJuliaTask");

    return !(FCConnect && FCDisconnect && FCSubmitRTask && FCSubmitJuliaTask);
}

//! \brief Helper function to clear function pointers
void ClearFunctionPtrs()
{
    FCConnect = nullptr;
    FCDisconnect = nullptr;
    FCSubmitRTask = nullptr;
    FCSubmitJuliaTask = nullptr;
}


//! \brief Unload fluid library.
void fluid::Shutdown()
{
    if (dllHandle != NULL)
    {
        FreeLibrary(dllHandle);
        dllHandle = NULL;
        ClearFunctionPtrs();
    }
}

//! \brief Connects to the remote server
//! \param ipAddress A string containing the ip address of the remote server
//! \param port The port of the remote server
void fluid::Connect(std::string const & ipAddress, int port)
{
    return FCConnect(ipAddress.c_str(), port);
}

//! \brief Disconnects from the remote server
void fluid::Disconnect()
{
    return FCDisconnect();
}

std::string fluid::SubmitRTask(std::vector<unsigned char> & buffer)
{
    char id[MAX_ID_LENGTH];
    memset(id, 0, sizeof(char) * MAX_ID_LENGTH);

    FCSubmitRTask(buffer.data(), buffer.size(), id, MAX_ID_LENGTH);
    return std::move(std::string(id));
}

std::string fluid::SubmitJuliaTask(std::vector<unsigned char> & buffer)
{
    #define MAX_ID_LENGTH  128
    char id[MAX_ID_LENGTH];
    memset(id, 0, sizeof(char) * MAX_ID_LENGTH);

    FCSubmitJuliaTask(buffer.data(), buffer.size(), id, MAX_ID_LENGTH);
    return std::move(std::string(id));
}
