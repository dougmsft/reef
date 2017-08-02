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

#ifndef fluid_client_h
#define fluid_client_h

#include <string>
#include <fluid/Export.h>

#ifdef __cplusplus
extern "C" {
#endif

    //namespace fluid {
    typedef enum
    {
        TaskQueued,
        TaskRunning,
        TaskComplete,
        TaskFailed
    } TaskStatus;

    FLUID_DECL void Connect(const char* ipAddress, int port);
    FLUID_DECL void Disconnect();
    FLUID_DECL void SubmitRTask(unsigned char* byteArray, int arraySize, char* id, int idLength);
    FLUID_DECL void SubmitJuliaTask(unsigned char* byteArray, int arraySize, char* id, int idLength);
    //FLUID_DECL TaskStatus GetRTaskResults(std::string const & taskId, unsigned char* byteArray, int & arraySize);
    //FLUID_DECL void GetJuliaTaskResults();
    //}

#ifdef __cplusplus
}
#endif

#endif // fluid_client_h
