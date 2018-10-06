include_directories(${CMAKE_SOURCE_DIR}/inc)
include_directories(${CMAKE_SOURCE_DIR}/../deps)
include_directories(${CMAKE_SOURCE_DIR}/../deps/h2o/include)
include_directories(${CMAKE_SOURCE_DIR}/../deps/lmdb)
include_directories(${CMAKE_SOURCE_DIR}/../deps/jansson/src)

link_directories(${CMAKE_SOURCE_DIR}/../deps/h2o)
link_directories(${CMAKE_SOURCE_DIR}/../deps/lmdb)
link_directories(${CMAKE_SOURCE_DIR}/../deps/jansson)

add_executable (marlin main.c marlin.c api.c app.c index.c shard.c sdata.c workers.c mapping.c)

target_link_libraries (marlin LINK_PUBLIC utils libjansson.a libh2o-evloop.a liblmdb.a jemalloc.a crypto ssl z pthread)
