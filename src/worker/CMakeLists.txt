project(RDDWorker)

set(WORKER_SOURCE_FILES
        worker.cc
        executor.h executor.cc
        rdd.h
        key_value_rdd.h
        key_values_rdd.h
        text_file_rdd.h
        net/socket_common.h
        net/socket_client.h
        net/socket_server.h
        net/socket_client_pool.h
        net/socket_non_blocking_server.h
        shuffle/block_manager.h shuffle/block_manager.cc
        shuffle/fully_connected_client.h shuffle/fully_connected_client.cc
        shuffle/fully_connected_server.h shuffle/fully_connected_server.cc
        shuffle/pairwise_shuffle_server.h shuffle/pairwise_shuffle_server.cc
        shuffle/pairwise_shuffle_client.h shuffle/pairwise_shuffle_client.cc
        udf.h
        rdd_env.cc rdd_env.h)

add_executable(RDDWorker ${WORKER_SOURCE_FILES})

target_link_libraries(RDDWorker msgpack jubatus_mpio jubatus_msgpack-rpc dl tbb pthread)

if (USE_RDMA)
    add_definitions(-DUSE_RDMA)
    target_link_libraries(RDDWorker rdmacm)
endif (USE_RDMA)