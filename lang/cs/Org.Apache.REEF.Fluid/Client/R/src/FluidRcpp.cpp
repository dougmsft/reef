// We can now use the BH package

// [[Rcpp::depends(BH)]]

//#undef Realloc
//#undef Free
//#include <windows.h>
//#undef ERROR
#include <Rcpp.h>
#include <sstream>
#include <exception>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>
//#include <fluid/Client.h>
#include "FluidClientInterface.h"


using namespace Rcpp;

// [[Rcpp::export(name=".initialize")]]
List rcpp_initialize(std::string const & packageDirectory) {
    int error = Initialize(packageDirectory);
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
        ConnectFoo("1.2.3.4", 20);
        //Connect(ipAddress.c_str(), port);
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
    boost::iostreams::array_source envSource((char*)&environment[0], environment.size());
    boost::iostreams::stream<boost::iostreams::array_source> envStream(envSource);

    boost::iostreams::array_source dataSource((char*)&dataFrame[0], dataFrame.size());
    boost::iostreams::stream<boost::iostreams::array_source> dataStream(dataSource);

    //fluid::SubmitRTask(dataStream, dataStream..length());

    return List::create(Named("env") = "test");

//return List::create(Named("env") = "test",
 //                       Named("data") = "test2");
}

