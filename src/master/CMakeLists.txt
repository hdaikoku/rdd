project(RDDMaster)

set(MASTER_SOURCE_FILES
        rdd_context.cc
        rdd_stub.h rdd_stub.cc
        key_value_rdd_stub.h key_value_rdd_stub.cc
        key_values_rdd_stub.h key_values_rdd_stub.cc
        text_file_rdd_stub.h text_file_rdd_stub.cc
        ../worker_context.h
        ../text_file_index.h
        ../rdd_rpc.h)

add_library(RDDMaster STATIC ${MASTER_SOURCE_FILES})

target_link_libraries(RDDMaster msgpack jubatus_mpio jubatus_msgpack-rpc)