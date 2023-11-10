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

#include <random>

#include "common/coding.h"

namespace LindormContest::test {

    static int32_t generate_random_int32(int32_t start, int32_t end) {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<int32_t> dis(start, end);
        return dis(gen);
    }

    TEST(DemoTest, DemoTest) {
        for (int WIDTH = 1; WIDTH <= 8; ++WIDTH) {
            const int32_t MIN = generate_random_int32(-10000, 10000);
            const int32_t MAX = MIN + std::pow(2, WIDTH) - 1;
            const int32_t N = 10000;

            std::vector<int32_t> src(N);
            std::vector<int32_t> dst(N);
            char compress_data[N * sizeof(int32_t)];

            for (int32_t i = 0; i < N; ++i) {
                src[i] = generate_random_int32(MIN, MAX);
            }

            bit_packing_encoding(WIDTH, MIN, src.data(), N, compress_data);
            bit_packing_decoding(WIDTH, MIN, compress_data, dst.data(), N);

            ASSERT_EQ(src, dst);
        }
    }

}