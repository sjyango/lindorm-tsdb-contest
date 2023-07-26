#
# Copyright Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
add_subdirectory(test)

file(GLOB_RECURSE sources_c "${PROJECT_SOURCE_DIR}/source/*.c")
file(GLOB_RECURSE sources_cpp "${PROJECT_SOURCE_DIR}/source/*.cpp")

add_library(DBLib ${sources_c} ${sources_cpp} test/demo_test.cpp)