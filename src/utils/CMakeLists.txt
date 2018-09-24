include_directories(../inc)

add_library (utils mlog.c)

target_include_directories (utils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
