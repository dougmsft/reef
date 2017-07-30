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
