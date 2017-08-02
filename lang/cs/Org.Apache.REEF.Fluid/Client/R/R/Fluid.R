#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#' Connect to a Fluid server.
#'
#' Connects the current instance of to the Fluid distributed compute server
#' located at the specified IP address and port number.
#'
#' @param ip A string that contains the internet protocal address of the Fluid
#' server in dotted decimal format.
#' @param port An integer that contains the port number of the Fluid server.
#'
#' @export
connect <- function(ip, port) {
  browser()
  # Get the package install directory.
  packageDir = dirname(getLoadedDLLs()$Fluid[['path']])
  x <- .initialize(packageDir)
  # Connect via C++
  .connect(ip, port)
}

#'
#' @export
disconnect <- function() {
    .disconnect()
}

#' Submits a job to the fluid server to run on a distributed cluster
#'
#' @export
submitJob <- function(func, data) {
    # create the new environment that we will send to the compute environment
    exportEnv <- new.env()
    exportEnv$func = func

    binaryEnv <- serialize_object(exportEnv)

    # save out the environment to memory
    binaryData <- serialize_object(data)

    .submitJob(binaryEnv, binaryData)
}

serialize_object <- function(object) {
    rds_buffer  <- rawConnection(raw(0),'w')
    on.exit(close(rds_buffer))
    saveRDS(object, rds_buffer)
    rawConnectionValue(rds_buffer)
}
