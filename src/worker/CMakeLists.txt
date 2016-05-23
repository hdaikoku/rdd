project(RDDWorker)

set(WORKER_SOURCE_FILES
        worker.cc
        ../worker_context.h
        executor.h executor.cc
        rdd.h rdd.cc
        key_value_rdd.h
        key_values_rdd.h
        text_file_rdd.h
        shuffle/block_manager.h shuffle/block_manager.cc
        #        shuffle/socket/socket_common.h
        #        shuffle/socket/socket_client.h
        #        shuffle/socket/socket_server.h
        #        shuffle/socket/socket_non_blocking_server.h
        shuffle/fully_connected_client.h shuffle/fully_connected_client.cc
        shuffle/fully_connected_server.h shuffle/fully_connected_server.cc
        shuffle/pairwise_shuffle_server.h shuffle/pairwise_shuffle_server.cc
        shuffle/pairwise_shuffle_client.h shuffle/pairwise_shuffle_client.cc)

add_executable(RDDWorker ${WORKER_SOURCE_FILES})

target_link_libraries(RDDWorker msgpack jubatus_mpio jubatus_msgpack-rpc dl tbb)