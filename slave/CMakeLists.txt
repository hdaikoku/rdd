project(RDDSlave)

set(SLAVE_SOURCE_FILES
        slave.cc
        ../include/slave_context.h
        ../include/slave/executor.h executor.cc
        ../include/slave/rdd.h rdd.cc
        ../include/slave/key_value_rdd.h
        ../include/slave/key_values_rdd.h
        ../include/slave/text_file_rdd.h
        ../include/slave/block_manager.h block_manager.cc
        ../include/socket/socket_common.h
        ../include/socket/socket_client.h
        ../include/socket/socket_server.h
        ../include/socket/non_blocking_server.h
        ../include/shuffle/fully_connected_client.h ../shuffle/fully_connected_client.cc
        ../include/shuffle/fully_connected_server.h ../shuffle/fully_connected_server.cc
        ../include/shuffle/pairwise_shuffle_server.h ../shuffle/pairwise_shuffle_server.cc
        ../include/shuffle/pairwise_shuffle_client.h ../shuffle/pairwise_shuffle_client.cc)

add_executable(RDDSlave ${SLAVE_SOURCE_FILES})

target_link_libraries(RDDSlave msgpack jubatus_mpio jubatus_msgpack-rpc dl tbb)