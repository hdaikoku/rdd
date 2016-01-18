project(RDDSlave)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DMP_FUNCTIONAL_STANDARD -DMP_MEMORY_STANDARD -DMP_UNORDERED_MAP_STANDARD")

include_directories(../include/slave)

set(SLAVE_SOURCE_FILES
        slave.cc
        ../include/slave_context.h
        ../include/slave/executor.h executor.cc
        ../include/slave/rdd.h rdd.cc
        ../include/slave/key_value_rdd.h
        ../include/slave/key_values_rdd.h
        ../include/slave/block_manager.h block_manager.cc
        ../socket/socket_common.cc
        ../socket/socket_server.cc
        ../socket/socket_client.cc
        ../include/slave/rpc_shuffle_server.h rpc_shuffle_server.cc
        ../include/slave/rpc_shuffle_client.h rpc_shuffle_client.cc
        ../include/slave/pairwise_shuffle_server.h pairwise_shuffle_server.cc
        ../include/slave/pairwise_shuffle_client.h pairwise_shuffle_client.cc)

add_executable(RDDSlave ${SLAVE_SOURCE_FILES})

target_link_libraries(RDDSlave ${MSGPACK_LIBRARY} jubatus_mpio jubatus_msgpack-rpc dl tbb)