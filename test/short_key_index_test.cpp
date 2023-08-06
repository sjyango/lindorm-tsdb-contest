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

#include <string>
#include <random>
#include <algorithm>

#include <gtest/gtest.h>

#include "common/slice.h"
#include "storage/indexs/short_key_index.h"

namespace LindormContest::test {

using namespace storage;

inline std::string generate_random_string(int length) {
    const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, charset.size() - 1);

    std::string str(length, '\0');
    for (int i = 0; i < length; ++i) {
        str[i] = charset[dis(gen)];
    }

    return str;
}

inline Slice lower_bound(const std::vector<std::string>& ss, const String& key) {
    for (const auto& item : ss) {
        if (item >= key) {
            return item;
        }
    }
    return "";
}

inline Slice upper_bound(const std::vector<std::string>& ss, const String& key) {
    for (const auto& item : ss) {
        if (item > key) {
            return item;
        }
    }
    return "";
}

TEST(ShortKeyIndexTest, BasicShortKeyIndexTest) {
    const size_t N = 100000;
    std::vector<std::string> ss;

    for (size_t i = 0; i < N; ++i) {
        ss.emplace_back(generate_random_string(17));
    }

    std::sort(ss.begin(), ss.end(), [](const Slice& lhs, const Slice& rhs) -> bool {
        return lhs.compare(rhs) < 0;
    });
    ShortKeyIndexWriter writer;

    for (const auto& key : ss) {
        writer.add_item(key);
    }

    OwnedSlice short_key_index_body;
    ShortKeyIndexFooter footer;
    writer.finalize(N, &short_key_index_body, &footer);
    ShortKeyIndexReader reader;
    reader.load(short_key_index_body.slice(), footer);
    auto it = ss.begin();

    for (auto iter = reader.begin(); iter != reader.end(); ++iter) {
        ASSERT_EQ(*iter, *it);
        ++it;
    }

    for (size_t i = 0; i < (N / 10); ++i) {
        std::string key = generate_random_string(17);
        auto lower_bound_lhs = reader.lower_bound(key);
        auto lower_bound_rhs = lower_bound(ss, key);
        ASSERT_EQ(lower_bound_lhs == reader.end(), lower_bound_rhs == "");
        if (lower_bound_lhs != reader.end()) {
            ASSERT_EQ(*lower_bound_lhs, lower_bound_rhs);
        }
        auto upper_bound_lhs = reader.upper_bound(key);
        auto upper_bound_rhs = upper_bound(ss, key);
        ASSERT_EQ(upper_bound_lhs == reader.end(), upper_bound_rhs == "");
        if (upper_bound_lhs != reader.end()) {
            ASSERT_EQ(*upper_bound_lhs, upper_bound_rhs);
        }
    }
}

TEST(ShortKeyIndexTest, EmptyShortKeyIndexTest) {
    const size_t N = 0;
    ShortKeyIndexWriter writer;
    OwnedSlice short_key_index_body;
    ShortKeyIndexFooter footer;
    writer.finalize(N, &short_key_index_body, &footer);
    ShortKeyIndexReader reader;
    reader.load(short_key_index_body.slice(), footer);
}

}

