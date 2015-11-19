cmake_minimum_required(VERSION 2.8)
project(MasterRDD)
project(SlaveRDD)
add_subdirectory(examples)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -DMP_FUNCTIONAL_STANDARD -DMP_MEMORY_STANDARD -DMP_UNORDERED_MAP_STANDARD")

include_directories(include)

set(COMMON_SOURCE_FILES include/rdd_rpc.h include/map_reduce.h)
set(MASTER_SOURCE_FILES
        master/master.cc
        master/rdd_context.cc master/rdd_context.h
        master/rdd_stub.cc master/rdd_stub.h
        master/text_rdd_stub.cc master/text_rdd_stub.h
        master/key_values_rdd_stub.cc master/key_values_rdd_stub.h)
set(SLAVE_SOURCE_FILES
        slave/slave.cc
        slave/executor.cc slave/executor.h
        slave/rdd.cc slave/rdd.h
        slave/text_rdd.cc slave/text_rdd.h
        slave/key_values_rdd.h
        socket/socket_common.h socket/socket_common.cc
        socket/socket_server.h socket/socket_server.cc
        socket/socket_client.h socket/socket_client.cc
        rdma/rdma_common.h rdma/rdma_common.cc
        rdma/rdma_server.h rdma/rdma_server.cc
        rdma/rdma_client.h rdma/rdma_client.cc)

add_executable(MasterRDD ${MASTER_SOURCE_FILES} ${COMMON_SOURCE_FILES})
add_executable(SlaveRDD ${SLAVE_SOURCE_FILES} ${COMMON_SOURCE_FILES})

target_link_libraries(MasterRDD msgpack jubatus_mpio jubatus_msgpack-rpc)
target_link_libraries(SlaveRDD msgpack jubatus_mpio jubatus_msgpack-rpc dl rdmacm)
