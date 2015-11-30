project(WordCountMapper)
project(WordCountReducer)
project(WordCountCombiner)
project(WordCount)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/../lib)

link_directories(${PROJECT_SOURCE_DIR}/../lib)

add_library(WordCountMapper SHARED word_count_mapper.cc word_count_mapper.h)
add_library(WordCountReducer SHARED word_count_reducer.cc word_count_reducer.h)
add_library(WordCountCombiner SHARED word_count_combiner.cc word_count_combiner.h)
add_executable(WordCount word_count.cc)

target_link_libraries(WordCountMapper tbb)
target_link_libraries(WordCount RDDMaster)