include_directories(${CMAKE_SOURCE_DIR}/inc)
include_directories(${CMAKE_SOURCE_DIR}/../deps)
include_directories(${CMAKE_SOURCE_DIR}/../deps/h2o/include)

link_directories(${CMAKE_SOURCE_DIR}/../deps/h2o)

add_executable (marlin main.c marlin.c api.c)

target_link_libraries (marlin LINK_PUBLIC utils libjansson.a libh2o-evloop.a jemalloc.a crypto ssl z pthread)
