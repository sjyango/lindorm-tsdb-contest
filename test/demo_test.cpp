/*
* Copyright Alibaba Group Holding Ltd.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#include <gtest/gtest.h>

#include "TSDBEngineImpl.h"

namespace LindormContest::test {

TEST(DemoTest, BasicDemoTest) {
    static const std::string PATH = "/home/ysj/lindorm-tsdb-contest-cpp/data";
    std::unique_ptr<TSDBEngineImpl> demo = std::make_unique<TSDBEngineImpl>(PATH);
    ASSERT_EQ(0, demo->connect());
}

}

