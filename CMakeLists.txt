cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -DMP_FUNCTIONAL_STANDARD -DMP_MEMORY_STANDARD -DMP_UNORDERED_MAP_STANDARD")

find_path(MSGPACK_INCLUDE_DIR NAMES msgpack.hpp PATHS
        /usr/include
        /usr/include/msgpack
        /usr/local/include
        /usr/local/include/msgpack
        $ENV{HOME}/local/include
        /usr/local/opt/msgpack059/include
        $ENV{HOME}/local/opt/msgpack059/include)

find_library(MSGPACK_LIBRARY NAMES msgpack PATHS
        /usr/lib
        /usr/local/lib
        /usr/local/lib/msgpack
        $ENV{HOME}/local/include
        /usr/local/opt/msgpack059/lib
        $ENV{HOME}/local/opt/msgpack059/lib)

include_directories(include ${MSGPACK_INCLUDE_DIR})

add_subdirectory(examples)
add_subdirectory(master)
add_subdirectory(slave)

