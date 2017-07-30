

#'
#' @export
initialize <- function() {
    # Get the package path by retrieving the path for the Fluid library
    fluidLib = getLoadedDLLs()['Fluid']
    libPath = dirname(fluidLib$Fluid[['path']])
    .initialize(libPath)
}


#'
#' @export
connect <- function(ip, port) {
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
