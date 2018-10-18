include_directories(../inc)

add_library (utils mlog.c utils.c threadpool.c base64.c flakeid.c cfarmhash.c)

target_include_directories (utils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
