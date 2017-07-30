#ifndef fluid_export_h
#define fluid_export_h

#if defined(_WIN32)
    #if defined(FLUID_DYN_LINK)
        #define FLUID_DECL  __declspec(dllexport)
    #else
        #define FLUID_DECL __declspec(dllimport)
    #endif // FLUID_DYN_LINK
#endif // _WIN32

#ifndef FLUID_DECL
#define FLUID_DECL
#endif

#endif
