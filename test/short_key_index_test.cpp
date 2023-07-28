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

std::string generate_random_string(int length) {
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

Slice lower_bound(const std::vector<std::string>& ss, const String& key) {
    for (const auto& item : ss) {
        if (item >= key) {
            return item;
        }
    }
    return "";
}

Slice upper_bound(const std::vector<std::string>& ss, const String& key) {
    for (const auto& item : ss) {
        if (item > key) {
            return item;
        }
    }
    return "";
}


TEST(ShortKeyIndexTest, BasicShortKeyIndexTest) {
    const size_t N = 10000;
    std::vector<std::string> ss;
    for (size_t i = 0; i < N; ++i) {
        ss.emplace_back(generate_random_string(17));
    }
    std::sort(ss.begin(), ss.end(), [](const Slice& lhs, const Slice& rhs) -> bool {
        return lhs.compare(rhs) < 0;
    });
    ShortKeyIndexWriter writer(0);
    for (const auto& key : ss) {
        writer.add_item(key);
    }
    std::shared_ptr<ShortKeyIndexPage> page = writer.finalize(ss.size());
    ShortKeyIndexReader reader;
    reader.parse(page.get());
    ShortKeyIndexReader::ShortKeyIndexIterator iter(&reader);

    for (const auto& item : ss) {
        ASSERT_EQ(*iter, item);
        ++iter;
    }

    for (size_t i = 0; i < (N / 10); ++i) {
        std::string key = generate_random_string(17);
        ASSERT_EQ(lower_bound(ss, key), *reader.lower_bound(key));
        ASSERT_EQ(upper_bound(ss, key), *reader.upper_bound(key));
    }
}

}

