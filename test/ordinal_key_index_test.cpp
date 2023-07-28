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
#include <algorithm>

#include <gtest/gtest.h>

#include "common/slice.h"
#include "storage/indexs/ordinal_key_index.h"

namespace LindormContest::test {

using namespace storage;

inline ordinal_t generate_random_ordinal(size_t range) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, range);
    return dis(gen);
}

inline ordinal_t lower_bound(const std::vector<ordinal_t>& ordinals, ordinal_t ordinal) {
    for (const auto& item : ordinals) {
        if (item >= ordinal) {
            return item;
        }
    }
    return 0;
}

inline ordinal_t upper_bound(const std::vector<ordinal_t>& ordinals, ordinal_t ordinal) {
    for (const auto& item : ordinals) {
        if (item > ordinal) {
            return item;
        }
    }
    return 0;
}

TEST(OrdinalKeyIndexTest, BasicOrdinalKeyIndexTest) {
    const size_t N = 10000;
    ordinal_t ordinal = 0;
    UInt32 index = 0;
    OrdinalIndexWriter writer;
    std::vector<ordinal_t> ordinals;

    for (size_t i = 0; i < N; ++i) {
        ordinals.push_back(ordinal);
        writer.append_entry(ordinal, index);
        ordinal += generate_random_ordinal(2000);
        index++;
    }

    std::shared_ptr<OrdinalIndexPage> page = writer.finalize();
    OrdinalIndexReader reader;
    reader.parse(page.get());
    OrdinalIndexReader::OrdinalPageIndexIterator iter(&reader);

    for (const auto& item : ordinals) {
        ASSERT_EQ(*iter, item);
        ++iter;
    }

    for (size_t i = 0; i < (N / 10); ++i) {
        ordinal_t o = generate_random_ordinal(ordinals.back());
        ASSERT_EQ(lower_bound(ordinals, o), *reader.lower_bound(o));
        ASSERT_EQ(upper_bound(ordinals, o), *reader.upper_bound(o));
    }
}

}

