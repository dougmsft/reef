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
