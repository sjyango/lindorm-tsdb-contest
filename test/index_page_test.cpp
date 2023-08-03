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

#include "io/io_utils.h"
#include "storage/indexs/index_page.h"

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

inline int32_t generate_random_int32() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int32_t> dis(0, std::numeric_limits<int32_t>::max());
    return dis(gen);
}

TEST(IndexPageTest, BasicIndexPageTest) {
    const int N = 1000;
    IndexPageEncoder encoder;
    IndexPageDecoder decoder;
    std::vector<std::string> keys;
    std::vector<io::PagePointer> values;

    for (int i = 0; i < N; ++i) {
        keys.emplace_back(generate_random_string(10));
        values.emplace_back(static_cast<uint64_t>(generate_random_int32() % 10000),
                            static_cast<uint32_t>(generate_random_int32() % 10000));
        encoder.add(keys.back(), values.back());
    }

    OwnedSlice data;
    IndexPageFooter footer;
    encoder.finish(&data, &footer);
    decoder.load(data.slice(), footer);

    for (int i = 0; i < N; ++i) {
        const Slice& key = decoder.get_key(i);
        ASSERT_EQ(key, keys[i]);
        const io::PagePointer& value = decoder.get_value(i);
        ASSERT_EQ(value, values[i]);
    }

    IndexPageIterator iter(&decoder);
    iter.seek_to_first();
    int i = 0;

    while (iter.has_next()) {
        const Slice& key = iter.current_key();
        ASSERT_EQ(key, keys[i]);
        const io::PagePointer& value = iter.current_page_pointer();
        ASSERT_EQ(value, values[i]);
        iter.next();
        i++;
    }

    ASSERT_EQ(i, N);
}

}

