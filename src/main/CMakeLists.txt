link_directories(${CMAKE_SOURCE_DIR}/../deps/h2o)
link_directories(${CMAKE_SOURCE_DIR}/../deps/lmdb)
link_directories(${CMAKE_SOURCE_DIR}/../deps/jansson/src/.libs)
link_directories(${CMAKE_SOURCE_DIR}/../deps/jemalloc/lib)
link_directories(${CMAKE_SOURCE_DIR}/../deps/utf8proc)

add_executable (marlin main.c marlin.c filter.c api.c app.c index.c shard.c sdata.c sindex.c workers.c mapping.c bmap.c cont.c dtrie.c mbmap.c query.c squery.c debug.c docrank.c)

if (DEBUG)
    target_link_libraries (marlin LINK_PUBLIC utils analyzer libjansson.a libh2o-evloop.a liblmdb.a jemalloc.a libutf8proc.a crypto ssl z pthread gcov)
else ()
    target_link_libraries (marlin LINK_PUBLIC utils analyzer liblmdb.a libjansson.a libh2o-evloop.a jemalloc.a libutf8proc.a crypto ssl z pthread)
endif ()
