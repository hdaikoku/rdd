cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -DMP_FUNCTIONAL_STANDARD -DMP_MEMORY_STANDARD -DMP_UNORDERED_MAP_STANDARD")

include_directories(include /usr/local/opt/msgpack059/include)
link_directories(/usr/local/opt/msgpack059/lib)

add_subdirectory(examples)
add_subdirectory(master)
add_subdirectory(slave)

