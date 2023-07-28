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

#include "common/bitmap.h"

namespace LindormContest::test {

TEST(BitMapTest, BasicBitMapTest1) {
    BitMap bm(100);
    bm.add_range(10, 20); // [10, 20)
    bm.add_range(30, 40); // [10, 20) + [30, 40)
    ASSERT_EQ("0000000000111111111100000000001111111111000000000000000000000000000000000000000000000000000000000000", bm.print());
    BitMap bm2(100);
    bm2.add_range(15, 25); // [15, 25)
    bm2.add_range(35, 45); // [15, 25) + [35, 45)
    bm.intersect(bm2); // [15, 20) + [35, 40)
    ASSERT_EQ("0000000000000001111100000000000000011111000000000000000000000000000000000000000000000000000000000000", bm.print());
}

TEST(BitMapTest, BasicBitMapTest2) {
    BitMap bm(100);
    bm.set(0);
    bm.set(31);
    bm.set(32);
    ASSERT_TRUE(bm.get(0));
    ASSERT_FALSE(bm.get(1));
    ASSERT_TRUE(bm.get(31));
    ASSERT_TRUE(bm.get(32));
    bm.add_range(50, 60); // [50, 60)
    ASSERT_FALSE(bm.get(49));
    ASSERT_TRUE(bm.get(50));
    ASSERT_TRUE(bm.get(55));
    ASSERT_FALSE(bm.get(60));
    BitMap bm2(100);
    bm2.add_range(40, 70); // [40, 70)
    bm.intersect(bm2); // [50, 60)
    ASSERT_FALSE(bm.get(0));
    ASSERT_FALSE(bm.get(31));
    ASSERT_FALSE(bm.get(32));
    ASSERT_TRUE(bm.get(50));
    ASSERT_TRUE(bm.get(55));
    ASSERT_FALSE(bm.get(60));
    ASSERT_FALSE(bm.get(61));
    ASSERT_FALSE(bm.get(69));
    ASSERT_FALSE(bm.get(70));
}

}

