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
project(tianchi-contest-semi-final)

set(CMAKE_BUILD_TYPE "DEBUG")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -fPIC -g -fopenmp")

set(CXX_FLAGS_ASAN "${CXX_GCC_FLAGS} -std=c++20 -O0 -fsanitize=address -DADDRESS_SANITIZER")

set(CXX_FLAGS_LSAN "${CXX_GCC_FLAGS} -std=c++20 -O0 -fsanitize=leak -DLEAK_SANITIZER")

if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_ASAN}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
    # export LD_LIBRARY_PATH='/home/ysj/gcc-11.1.0/x86_64-pc-linux-gnu/libsanitizer/asan/.libs/'
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_LSAN}")
    # LD_PRELOAD=/home/ysj/gcc-11.1.0/x86_64-pc-linux-gnu/libsanitizer/lsan/.libs/liblsan.so.0 ./multi_thread_test
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

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

add_subdirectory(test)

file(GLOB_RECURSE sources_c "${PROJECT_SOURCE_DIR}/source/*.c")
file(GLOB_RECURSE sources_cpp "${PROJECT_SOURCE_DIR}/source/*.cpp")

add_library(DBLib ${sources_c} ${sources_cpp} include/common/coding.h)
