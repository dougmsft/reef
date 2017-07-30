#include <fluid/Client.h>
#include <fluid/RTaskMsg.h>
#include <fluid/JuliaTaskMsg.h>
#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>



//! \brief Makes a connection to the remote server
//! \detailed Connects to the remote server given the ip address and port. If the connection fails or times out
//! an exception is thrown 
//! \param ipAddress The ip address of the remote server
//! \param port The port of the remote server
void Connect(const char* ipAddress, int port)
{
}

//! \brief Disconnects an existing connection.
void Disconnect()
{
}

//! \brief Submits an R task to the remote server
//! \param byteArray

void SubmitRTask(unsigned char* byteArray, int arraySize, char* id, int idLength)
{
    fluid::RTaskMsg rTaskMsg;
    rTaskMsg.data = "data";
    rTaskMsg.function = "function";
    rTaskMsg.uuid = "generatdid";
    rTaskMsg.uuid.copy(id, idLength);
    return;
}

//! \brief Retrieves the results of an R task
TaskStatus GetRTaskResults(std::string const & taskId, unsigned char* byteArray, int & arraySize)
{
    return TaskQueued;
}

void SubmitJuliaTask(unsigned char* byteArray, int arraySize, char* id, int idLength)
{
    fluid::JuliaTaskMsg juliaTaskMsg;
    juliaTaskMsg.data = "data";
    juliaTaskMsg.function = "function";
    juliaTaskMsg.uuid = "generatedId";
    juliaTaskMsg.uuid.copy(id, idLength);
    return;
}

void GetJuliaTaskResults()
{
}