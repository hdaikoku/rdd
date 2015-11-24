project(RDDSlave)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DMP_FUNCTIONAL_STANDARD -DMP_MEMORY_STANDARD -DMP_UNORDERED_MAP_STANDARD")

set(SLAVE_SOURCE_FILES
        slave.cc
        executor.cc
        rdd.cc
        ../socket/socket_common.cc
        ../socket/socket_server.cc
        ../socket/socket_client.cc)

add_executable(RDDSlave ${SLAVE_SOURCE_FILES})

target_link_libraries(RDDSlave msgpack jubatus_mpio jubatus_msgpack-rpc dl tbb)