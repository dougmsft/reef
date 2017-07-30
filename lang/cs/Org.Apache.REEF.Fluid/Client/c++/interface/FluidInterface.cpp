#include <windows.h>
#include <vector>
#include "FluidInterface.h"


static HINSTANCE dllHandle;
#define MAX_ID_LENGTH  128

// Library function declarations 
typedef void (*ConnectFunc)(const char* ipAddress, int port);
typedef void (*DisconnectFunc)();
typedef const char* (*SubmitRTaskFunc)(const unsigned char* byteArray, int size, char* id, int idLength);
typedef const char* (*SubmitJuliaTaskFunc)(const unsigned char* byteArray, int size, char* id, int idLength);


static ConnectFunc FCConnect = nullptr;
static DisconnectFunc FCDisconnect = nullptr;
static SubmitRTaskFunc FCSubmitRTask = nullptr;
static SubmitJuliaTaskFunc FCSubmitJuliaTask = nullptr;

std::string GetWorkingDirectory()
{
    TCHAR Buffer[MAX_PATH];
    DWORD dwRet;

    dwRet = GetCurrentDirectory(MAX_PATH, Buffer);

    return std::string(Buffer);
}

//! \brief Initializes the library and function pointers
//! \detailed The fluidclient library is loaded at runtime. This will attempt to load the library given the 
//!    working directory and find the functions within the library.
//! \param workingDirectory the directory of where to find the library 
//! \return 0 if successful, non-zero if not successful.
int fluid::Initialize(std::string const& workingDirectory)
{
    std::string fullPath = workingDirectory + "/fluidclient.dll";
    dllHandle = LoadLibrary(fullPath.c_str());
    if (dllHandle == NULL)
    {
        return (int)GetLastError();
    }

    FCConnect = (ConnectFunc)GetProcAddress(dllHandle, "Connect");
    FCDisconnect = (DisconnectFunc)GetProcAddress(dllHandle, "Disconnect");
    FCSubmitRTask = (SubmitRTaskFunc)GetProcAddress(dllHandle, "SubmitRTask");
    FCSubmitJuliaTask = (SubmitJuliaTaskFunc)GetProcAddress(dllHandle, "SubmitJuliaTask");

    return 0;
}

//! \brief Helper function to clear function pointers
void ClearFunctionPtrs()
{
    FCConnect = nullptr;
    FCDisconnect = nullptr;
    FCSubmitRTask = nullptr;
    FCSubmitJuliaTask = nullptr;
}


//! \brief Shutsdown the library and clears function pointers
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
