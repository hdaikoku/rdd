cmake_minimum_required(VERSION 3.0)
project(WordCountMapper)
project(WordCountReducer)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fPIC")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR})

include_directories(../include)

add_library(WordCountMapper SHARED word_count_mapper.cc word_count_mapper.h)
add_library(WordCountReducer SHARED word_count_reducer.cc word_count_reducer.h)

target_link_libraries(WordCountMapper tbb)