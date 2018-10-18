if ( ${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR} )                                                   
    message( FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build direct
ory) and run CMake from there. You may need to remove CMakeCache.txt." )                                  
endif()                                                                                                   
cmake_minimum_required(VERSION 2.7)                                                                       
project (MARLIN)

set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/../cmake)

if (DEBUG)
include(CodeCoverage)
SETUP_TARGET_FOR_COVERAGE(marlin_coverage "python ../test/test.py" coverage)
endif ()

include_directories(${CMAKE_SOURCE_DIR}/inc)
include_directories(${CMAKE_SOURCE_DIR}/../deps)
include_directories(${CMAKE_SOURCE_DIR}/../deps/h2o/include)
include_directories(${CMAKE_SOURCE_DIR}/../deps/lmdb)
include_directories(${CMAKE_SOURCE_DIR}/../deps/utf8proc)
include_directories(${CMAKE_SOURCE_DIR}/../deps/jansson/src)

SET(CMAKE_C_FLAGS_DEBUG "-std=gnu99 -Wall -g -O0 --coverage -fprofile-arcs -ftest-coverage -march=native -DH20_USE_EPOLL -DH2O_USE_LIBUV=0 -D_GNU_SOURCE -DDEBUG_BUILD -pthread")

SET(CMAKE_C_FLAGS_RELEASE "-std=gnu99 -pthread -Wall -g -Ofast -march=native -DH20_USE_EPOLL -DH2O_USE_LIBUV=0 -D_GNU_SOURCE")

add_subdirectory (analyzer)
add_subdirectory (utils)
add_subdirectory (main)
