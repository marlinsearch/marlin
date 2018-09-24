if ( ${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR} )                                                   
    message( FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build direct
ory) and run CMake from there. You may need to remove CMakeCache.txt." )                                  
endif()                                                                                                   
cmake_minimum_required(VERSION 2.7)                                                                       
project (MARLIN)

SET(CMAKE_C_FLAGS_DEBUG "-std=gnu99 -Wall -g -O0 -march=native -DH20_USE_EPOLL -DH2O_USE_LIBUV=0 -D_GNU_SOURCE -pthread")

SET(CMAKE_C_FLAGS_RELEASE "-std=gnu99 -pthread -Wall -g -Ofast -march=native -DH20_USE_EPOLL -DH2O_USE_LIBUV=0 -D_GNU_SOURCE")

add_subdirectory (utils)
add_subdirectory (main)
