project(RDDMaster)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/../lib)

set(MASTER_SOURCE_FILES
        ../include/master/rdd_context.h rdd_context.cc
        ../include/master/rdd_stub.h rdd_stub.cc
        ../include/master/key_value_rdd_stub.h key_value_rdd_stub.cc
        ../include/master/key_values_rdd_stub.h key_values_rdd_stub.cc
        ../include/master/text_file_rdd_stub.h text_file_rdd_stub.cc
        ../include/slave_context.h
        ../include/text_file_index.h
        ../include/rdd_rpc.h ../include/shuffle/send_buffer.h)

add_library(RDDMaster STATIC ${MASTER_SOURCE_FILES})

target_link_libraries(RDDMaster msgpack jubatus_mpio jubatus_msgpack-rpc)