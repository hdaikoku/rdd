project(WordCountMapper)
project(WordCountReducer)
project(WordCount)
project(BiGramMapper)
project(BiGramReducer)
project(GroupByMapper)
project(GroupBy)


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

# WordCount
add_library(WordCountMapper SHARED word_count_mapper.cc word_count_mapper.h)
add_library(WordCountReducer SHARED word_count_reducer.cc word_count_reducer.h)
add_executable(WordCount word_count.cc)
target_link_libraries(WordCount RDDMaster)

# BiGram
add_library(BiGramMapper SHARED bi_gram_mapper.cc bi_gram_mapper.h)
add_library(BiGramReducer SHARED bi_gram_reducer.cc bi_gram_reducer.h)

# GroupBy
add_library(GroupByMapper SHARED group_by_mapper.cc group_by_mapper.h)
add_executable(GroupBy group_by.cc)
target_link_libraries(GroupBy RDDMaster)