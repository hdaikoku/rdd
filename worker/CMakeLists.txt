project(RDDWorker)

set(WORKER_SOURCE_FILES
        worker.cc
        ../include/worker_context.h
        ../include/worker/executor.h executor.cc
        ../include/worker/rdd.h rdd.cc
        ../include/worker/key_value_rdd.h
        ../include/worker/key_values_rdd.h
        ../include/worker/text_file_rdd.h
        ../include/worker/block_manager.h block_manager.cc
        ../include/socket/socket_common.h
        ../include/socket/socket_client.h
        ../include/socket/socket_server.h
        ../include/socket/socket_non_blocking_server.h
        ../include/shuffle/fully_connected_client.h ../shuffle/fully_connected_client.cc
        ../include/shuffle/fully_connected_server.h ../shuffle/fully_connected_server.cc
        ../include/shuffle/pairwise_shuffle_server.h ../shuffle/pairwise_shuffle_server.cc
        ../include/shuffle/pairwise_shuffle_client.h ../shuffle/pairwise_shuffle_client.cc)

add_executable(RDDWorker ${WORKER_SOURCE_FILES})

target_link_libraries(RDDWorker msgpack jubatus_mpio jubatus_msgpack-rpc dl tbb)