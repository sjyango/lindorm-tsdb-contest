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

#include "common/coding.h"

namespace LindormContest::test {
    TEST(DemoTest, DemoTest) {
        std::vector<int64_t> tss {12345, 12346, 12347, 12348, 12349};
        std::string buf1;
        std::string buf2;
        buf1.append(reinterpret_cast<const char *>(tss.data()), tss.size() * sizeof(int64_t));

        for (const auto &ts: tss) {
            put_fixed(&buf2, ts);
        }

        ASSERT_EQ(buf1, buf2);


    }
}