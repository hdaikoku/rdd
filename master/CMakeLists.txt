project(RDDMaster)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/../lib)

set(MASTER_SOURCE_FILES
        rdd_context.cc
        key_value_rdd_stub.cc
        key_values_rdd_stub.cc
        ../include/slave_context.h)

add_library(RDDMaster STATIC ${MASTER_SOURCE_FILES})

target_link_libraries(RDDMaster msgpack jubatus_mpio jubatus_msgpack-rpc)