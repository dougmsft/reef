cmake_minimum_required (VERSION 2.6)

set (CMAKE_LEGACY_CYGWIN_WIN32 0)

if (NOT DEFINED CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS_NO_WARNINGS)
    set (CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS_NO_WARNINGS ON)
endif()

project (FluidCoreClient)

set(FLUID_VERSION_MAJOR "0")
set(FLUID_VERSION_MINOR "1")

if (WIN32 AND NOT CYGWIN AND NOT MSYS)
add_definitions (/EHa)
add_definitions (
    -DBOOST_REGEX_DYN_LINK
    -DBOOST_FILESYSTEM_DYN_LINK
    -DBOOST_SYSTEM_DYN_LINK
    -DBOOST_IOSTREAMS_DYN_LINK
    -DBOOST_PROGRAM_OPTIONS_DYN_LINK
    -DBOOST_ALL_NO_LIB)
endif()

if (CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "-Wall")
    set(CMAKE_CXX_FLAGS_DEBUG "-fstack-protector-all -D_GLIBCXX_DEBUG")
endif ()


# Set boost include and libraries
set(BOOST_INCLUDEDIR "packages/boost.1.64.0.0/lib/native/include")
set(BOOST_LIBRARYDIR "packages/boost.1.64.0.0/lib/native/address-model-64")
find_package (Boost 1.38 REQUIRED
    COMPONENTS filesystem system program_options iostreams zlib bzip2)
add_definitions (${Boost_LIB_DIAGNOSTIC_DEFINITIONS})

# Set Avro include and libraries
set(AVRO_INCLUDE_DIR "packages/avro/build/native/include")
set(AVRO_LIBRARY_DIR "packages/avro/lib/native/Windows/x64")
set(AVRO_TOOLS_DIR "packages/avro/tools/native/Windows/x64")
include_directories (include interface ${AVRO_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${Boost_INCLUDE_DIRS})
find_library(AVRO_LIBRARIES avrocpp_s HINTS ${AVRO_LIBRARY_DIR})
find_program(AVROGEN_CPP avrogencpp HINTS ${AVRO_TOOLS_DIR})

message("found avrogen: ${AVROGEN_CPP}")

macro (gen file ns)
    add_custom_command (OUTPUT ${file}.h
        COMMAND ${AVROGEN_CPP}
            -p Avro
            -i ${CMAKE_CURRENT_SOURCE_DIR}/../../Avro/${file}.avsc
            -o ${CMAKE_CURRENT_SOURCE_DIR}/include/fluid/message/${file}.h -n ${ns} -U
        DEPENDS avrogencpp ${CMAKE_CURRENT_SOURCE_DIR}/../../Avro/${file})
    add_custom_target (${file} DEPENDS ${file}.h)
endmacro (gen)

gen (JuliaResultsMsg fluid)
gen (JuliaTaskMsg fluid)
gen (RResultsMsg fluid)
gen (RTaskMsg fluid)
gen (ShutdownMsg fluid)

set (FLUID_SOURCE_FILES
        src/Client.cpp
        )

set (FLUID_HEADER_FILES
        include/fluid/Client.h
        include/fluid/Export.h
        )

add_library (fluid_core_client SHARED ${FLUID_SOURCE_FILES} ${FLUID_HEADER_FILES})
add_dependencies (fluid_core_client JuliaResultsMsg JuliaTaskMsg RResultsMsg RTaskMsg ShutdownMsg)

set_property (TARGET fluid_core_client
    APPEND PROPERTY COMPILE_DEFINITIONS FLUID_DYN_LINK)

set_property (TARGET fluid_core_client
    APPEND PROPERTY COMPILE_DEFINITIONS FLUID_SOURCE)

set_target_properties (fluid_core_client PROPERTIES
    VERSION ${FLUID_VERSION_MAJOR}.${FLUID_VERSION_MINOR})

target_link_libraries (fluid_core_client ${AVRO_LIBRARIES} ${Boost_LIBRARIES})

# build test
add_executable(fluidclient_test test/LoadLibrary.cpp interface/FluidCoreInterface.cpp interface/FluidCoreInterface.h)
target_link_libraries(fluidclient_test fluid_core_client ${AVRO_LIBRARIES})
add_dependencies(fluidclient_test fluid_core_client)

set(R_SRC_DIR "../R/src")
add_custom_target(interface ALL cp ${CMAKE_SOURCE_DIR}/interface/FluidCoreInterface.cpp ${CMAKE_SOURCE_DIR}/${R_SRC_DIR}
    COMMAND cp ${CMAKE_SOURCE_DIR}/interface/FluidCoreInterface.h ${CMAKE_SOURCE_DIR}/${R_SRC_DIR})
install(TARGETS fluid_core_client DESTINATION ${CMAKE_SOURCE_DIR}/${R_SRC_DIR} CONFIGURATIONS Debug Release MinSizeRel RelWithDebInfo)

if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release CACHE STRING
      "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel."
      FORCE)
endif (NOT CMAKE_BUILD_TYPE)
