project(RDDMaster)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DMP_FUNCTIONAL_STANDARD -DMP_MEMORY_STANDARD -DMP_UNORDERED_MAP_STANDARD")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/../lib)

set(MASTER_SOURCE_FILES
        rdd_context.cc
        key_value_rdd_stub.cc
        key_values_rdd_stub.cc)

add_library(RDDMaster STATIC ${MASTER_SOURCE_FILES})

target_link_libraries(RDDMaster ${MSGPACK_LIBRARY} jubatus_mpio jubatus_msgpack-rpc)