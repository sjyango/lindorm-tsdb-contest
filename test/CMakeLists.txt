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

aux_source_directory(. TEST_LIST)

find_package(Threads REQUIRED)

include_directories(${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/thirdparty/gtest/include)

link_directories(${PROJECT_SOURCE_DIR}/thirdparty/gtest/lib)

set(EXECUTABLES
        demo_test
        io_test
        tsm_test
        multi_thread_test
        )

foreach(EXECUTABLE ${EXECUTABLES})
    add_executable(${EXECUTABLE} ${EXECUTABLE}.cpp)
    target_link_libraries(${EXECUTABLE} PRIVATE Threads::Threads gtest gtest_main DBLib)
endforeach()

add_executable(ut_test ${TEST_LIST})
target_link_libraries(ut_test Threads::Threads gtest gtest_main DBLib)
