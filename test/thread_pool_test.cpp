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

#include <random>

#include <gtest/gtest.h>

#include "common/thread_pool.h"

namespace LindormContest::test {

    static void simulate_hard_computation() {
        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<int> dist(0, 500);
        auto rnd = [&] { return dist(mt); };
        std::this_thread::sleep_for(std::chrono::milliseconds(rnd()));
    }

    static void multiply(const int a, const int b) {
        simulate_hard_computation();
        const int res = a * b;
        std::cout << a << " * " << b << " = " << res << std::endl;
    }

    static void multiply_output(int &out, const int a, const int b) {
        simulate_hard_computation();
        out = a * b;
        std::cout << a << " * " << b << " = " << out << std::endl;
    }

    static int multiply_return(const int a, const int b) {
        simulate_hard_computation();
        const int res = a * b;
        std::cout << a << " * " << b << " = " << res << std::endl;
        return res;
    }

    TEST(ThreadPoolTest, BasicThreadPoolTest) {
        ThreadPool pool(3);
        pool.init();

        for (int i = 0; i < 10; ++i) {
            for (int j = 0; j < 10; ++j) {
                pool.submit(multiply, i, j);
            }
        }

        int output_ref;
        auto future1 = pool.submit(multiply_output, std::ref(output_ref), 5, 6);
        future1.get();
        ASSERT_EQ(output_ref, 30);
        std::cout << "Last operation result is equals to " << output_ref << std::endl;

        auto future2 = pool.submit(multiply_return, 5, 3);
        int res = future2.get();
        ASSERT_EQ(res, 15);
        std::cout << "Last operation result is equals to " << res << std::endl;

        pool.shutdown();
    }
}