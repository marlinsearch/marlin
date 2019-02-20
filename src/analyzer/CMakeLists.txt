include_directories(../inc)

add_library (analyzer analyzer.c default_analyzer.c whitespace_analyzer.c word.c)

target_include_directories (analyzer PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
