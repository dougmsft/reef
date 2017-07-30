// We can now use the BH package

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
//#include <fluid/Client.h>
#include "FluidInterface.h"


using namespace Rcpp;

// [[Rcpp::export(name=".initialize")]]
List rcpp_initialize(std::string const & packageDirectory) {
    int error = fluid::Initialize(packageDirectory);
    if (error > 0)
    {
        return List::create(Named("error") = error,
                            Named("dir") = packageDirectory);
    }

    return List::create(Named("dir") = packageDirectory);
}

List rcpp_shutdown(){

}

// [[Rcpp::export(name=".connect")]]
List rcpp_connect(std::string const & ipAddress, int port) {

    try
    {
        fluid::Connect(ipAddress.c_str(), port);
    }
    catch(std::exception ex)
    {
        return List::create(Named("connected") = false,
                            Named("error") = ex.what());
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

