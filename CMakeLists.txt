cmake_minimum_required(VERSION 2.8.12)
project(lindorm-tsdb-contest-cpp)

# We use g++-12 to compile, so that we recommend you to use the same compiler to test for better compatibility.
message("Your OS: ${CMAKE_HOST_SYSTEM_NAME}")
if (CMAKE_HOST_SYSTEM_NAME MATCHES "Darwin")
    set(CMAKE_C_COMPILER "gcc-12")
    set(CMAKE_CXX_COMPILER "g++-12")
elseif (MAKE_HOST_SYSTEM_NAME MATCHES "Windows")
    # Add w64devkit\bin to PATH.
else ()
    set(CMAKE_C_COMPILER "gcc")
    set(CMAKE_CXX_COMPILER "g++")
endif ()

include_directories(${PROJECT_SOURCE_DIR}/include)
add_subdirectory(example)

file(GLOB_RECURSE sources_c "${PROJECT_SOURCE_DIR}/source/*.c")
file(GLOB_RECURSE sources_cpp "${PROJECT_SOURCE_DIR}/source/*.cpp")

add_library(yourDbLib ${sources_c} ${sources_cpp} include/storage/table_schema.h include/vec/datatypes/IDataType.h include/vec/datatypes/DataTypeString.h include/vec/columns/ColumnFactory.h include/vec/datatypes/DataTypeFactory.h include/vec/datatypes/DataTypeInt32.h include/vec/datatypes/DataTypeFloat64.h include/storage/memtable.h source/storage/memtable.cpp include/vec/columns/column_with_type_and_name.h include/storage/delta_writer.h source/storage/delta_writer.cpp include/storage/rowset_writer.h source/storage/rowset_writer.cpp include/storage/segment_writer.h source/storage/segment_writer.cpp include/storage/column_writer.h source/storage/column_writer.cpp include/storage/segment_traits.h include/storage/page_builder.h include/storage/plain_page_builder.h include/common/coding.h include/storage/indexs/ordinal_page_index.h include/storage/index_page_builder.h include/storage/page.h include/storage/indexs/short_key_index.h include/storage/data_convertor.h include/storage/indexs/key_coder.h)
