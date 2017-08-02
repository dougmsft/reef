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

// [[Rcpp::depends(BH)]]

//#undef Realloc
//#undef Free
//#include <windows.h>
//#undef ERROR

#include <Rcpp.h>
#include <sstream>
#include <vector>
#include <exception>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>
#include "FluidCoreInterface.h"

using namespace Rcpp;


// [[Rcpp::export(name=".initialize")]]
List rcpp_initialize(std::string const & packageDirectory) {
    int error = fluid::Initialize(packageDirectory);
    if (error > 0)
    {
        return List::create(Named("error") = error, Named("dir") = packageDirectory);
    }
    return List::create(Named("dir") = packageDirectory);
}

List rcpp_shutdown(){
    return NULL;
}

// [[Rcpp::export(name=".connect")]]
List rcpp_connect(std::string const & ipAddress, int port) {

    try
    {
        fluid::Connect(ipAddress.c_str(), port);
    }
    catch(std::exception ex)
    {
        return List::create(Named("connected") = false, Named("error") = ex.what());
    }
    return List::create(Named("connected") = true);
}


// [[Rcpp::export(name=".disconnect")]]
List rcpp_disconnect() {
    try
    {
        //Disconnect();
    }
    catch(std::exception ex)
    {
        return List::create(Named("connected") = false,
                            Named("error") = ex.what());
    }
    return List::create(Named("connected") = false);
}


// [[Rcpp::export(name=".submitJob")]]
List rcpp_submitJob(const Rcpp::RawVector & environment, const Rcpp::RawVector & dataFrame) {
    std::vector<unsigned char> envVec(environment.length());
    std::copy(environment.begin(), environment.end(), envVec.begin());
    std::vector<unsigned char> dataVec(dataFrame.length());
    std::copy(dataFrame.begin(), dataFrame.end(), dataVec.begin());

    std::string jobId = fluid::SubmitRTask(envVec);
    return List::create(Named("id") = jobId);
}
