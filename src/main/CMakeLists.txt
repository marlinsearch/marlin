link_directories(${CMAKE_SOURCE_DIR}/../deps/h2o)
link_directories(${CMAKE_SOURCE_DIR}/../deps/lmdb)
link_directories(${CMAKE_SOURCE_DIR}/../deps/jansson/src/.libs)
link_directories(${CMAKE_SOURCE_DIR}/../deps/jemalloc/lib)
link_directories(${CMAKE_SOURCE_DIR}/../deps/utf8proc)

add_executable (marlin main.c marlin.c api.c app.c index.c shard.c sdata.c workers.c mapping.c)

target_link_libraries (marlin LINK_PUBLIC utils libjansson.a libh2o-evloop.a liblmdb.a jemalloc.a crypto ssl z pthread)
