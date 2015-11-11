cmake_minimum_required(VERSION 3.0)
project(WordCount)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fPIC")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR})

include_directories(../include)

set(WORD_COUNT_SOURCE_FILES word_count_map_reduce.cc word_count_map_reduce.h)

add_library(WordCount SHARED ${WORD_COUNT_SOURCE_FILES})