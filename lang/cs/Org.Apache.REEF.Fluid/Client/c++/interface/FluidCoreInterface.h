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

#ifndef FLUID_LIBRARY_INTERFACE_H
#define FLUID_LIBRARY_INTERFACE_H

#include <string>
#include <vector>

namespace fluid {
    std::string GetWorkingDirectory();
    int Initialize(std::string const & workingDirectory);
    void Shutdown();
    void Connect(std::string const & ipAddress, int port);
    void Disconnect();

    std::string SubmitRTask(std::vector<unsigned char> & buffer);
    std::string SubmitJuliaTask(std::vector<unsigned char> & buffer);
}

#endif // FLUID_LIBRARY_INTERFACE_H
