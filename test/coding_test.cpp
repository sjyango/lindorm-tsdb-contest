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

TEST(Coding, Fixed32) {
    std::string s;

    for (uint32_t v = 0; v < 100000; v++) {
        put_fixed32_le(&s, v);
    }

    const uint8_t* p = (const uint8_t*)s.data();

    for (uint32_t v = 0; v < 100000; v++) {
        uint32_t actual = decode_fixed32_le(p);
        ASSERT_EQ(v, actual);
        p += sizeof(uint32_t);
    }
}

TEST(Coding, Fixed64) {
    std::string s;

    for (int power = 0; power <= 63; power++) {
        uint64_t v = static_cast<uint64_t>(1) << power;
        put_fixed64_le(&s, v - 1);
        put_fixed64_le(&s, v + 0);
        put_fixed64_le(&s, v + 1);
    }

    const uint8_t* p = (const uint8_t*)s.data();

    for (int power = 0; power <= 63; power++) {
        uint64_t v = static_cast<uint64_t>(1) << power;
        uint64_t actual;
        actual = decode_fixed64_le(p);
        ASSERT_EQ(v - 1, actual);
        p += sizeof(uint64_t);

        actual = decode_fixed64_le(p);
        ASSERT_EQ(v + 0, actual);
        p += sizeof(uint64_t);

        actual = decode_fixed64_le(p);
        ASSERT_EQ(v + 1, actual);
        p += sizeof(uint64_t);
    }
}

// Test that encoding routines generate little-endian encodings
TEST(Coding, EncodingOutput) {
    std::string dst;
    put_fixed32_le(&dst, 0x04030201);
    ASSERT_EQ(4, dst.size());
    ASSERT_EQ(0x01, static_cast<int>(dst[0]));
    ASSERT_EQ(0x02, static_cast<int>(dst[1]));
    ASSERT_EQ(0x03, static_cast<int>(dst[2]));
    ASSERT_EQ(0x04, static_cast<int>(dst[3]));

    dst.clear();
    put_fixed64_le(&dst, 0x0807060504030201ull);
    ASSERT_EQ(8, dst.size());
    ASSERT_EQ(0x01, static_cast<int>(dst[0]));
    ASSERT_EQ(0x02, static_cast<int>(dst[1]));
    ASSERT_EQ(0x03, static_cast<int>(dst[2]));
    ASSERT_EQ(0x04, static_cast<int>(dst[3]));
    ASSERT_EQ(0x05, static_cast<int>(dst[4]));
    ASSERT_EQ(0x06, static_cast<int>(dst[5]));
    ASSERT_EQ(0x07, static_cast<int>(dst[6]));
    ASSERT_EQ(0x08, static_cast<int>(dst[7]));
}

TEST(Coding, Varint32) {
    std::string s;

    for (uint32_t i = 0; i < (32 * 32); i++) {
        uint32_t v = (i / 32) << (i % 32);
        put_varint32(&s, v);
    }

    const uint8_t* p = (const uint8_t*)s.data();
    const uint8_t* limit = p + s.size();

    for (uint32_t i = 0; i < (32 * 32); i++) {
        uint32_t expected = (i / 32) << (i % 32);
        uint32_t actual;
        const uint8_t* start = p;
        p = decode_varint32_ptr(p, limit, &actual);
        ASSERT_TRUE(p != nullptr);
        ASSERT_EQ(expected, actual);
        ASSERT_EQ(varint_length(actual), p - start);
    }
    ASSERT_EQ(p, (const uint8_t*)s.data() + s.size());
}

TEST(Coding, Varint64) {
    // Construct the list of values to check
    std::vector<uint64_t> values;
    // Some special values
    values.push_back(0);
    values.push_back(100);
    values.push_back(~static_cast<uint64_t>(0));
    values.push_back(~static_cast<uint64_t>(0) - 1);

    for (uint32_t k = 0; k < 64; k++) {
        // Test values near powers of two
        const uint64_t power = 1ull << k;
        values.push_back(power);
        values.push_back(power - 1);
        values.push_back(power + 1);
    }

    std::string s;

    for (size_t i = 0; i < values.size(); i++) {
        put_varint64(&s, values[i]);
    }

    const uint8_t* p = (const uint8_t*)s.data();
    const uint8_t* limit = p + s.size();

    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_TRUE(p < limit);
        uint64_t actual;
        const uint8_t* start = p;
        p = decode_varint64_ptr(p, limit, &actual);
        ASSERT_TRUE(p != nullptr);
        ASSERT_EQ(values[i], actual);
        ASSERT_EQ(varint_length(actual), p - start);
    }

    ASSERT_EQ(p, limit);
}

TEST(Coding, Varint32Overflow) {
    uint32_t result;
    std::string input("\x81\x82\x83\x84\x85\x11");
    ASSERT_TRUE(decode_varint32_ptr((const uint8_t*)input.data(), (const uint8_t*)input.data() + input.size(),
                               &result) == nullptr);
}

TEST(Coding, Varint32Truncation) {
    uint32_t large_value = (1u << 31) + 100;
    std::string s;
    put_varint32(&s, large_value);
    uint32_t result;
    for (size_t len = 0; len < s.size() - 1; len++) {
        ASSERT_TRUE(decode_varint32_ptr((const uint8_t*)s.data(), (const uint8_t*)s.data() + len, &result) == nullptr);
    }
    ASSERT_TRUE(decode_varint32_ptr((const uint8_t*)s.data(), (const uint8_t*)s.data() + s.size(), &result) != nullptr);
    ASSERT_EQ(large_value, result);
}

TEST(Coding, Varint64Overflow) {
    uint64_t result;
    std::string input("\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11");
    ASSERT_TRUE(decode_varint64_ptr((const uint8_t*)input.data(), (const uint8_t*)input.data() + input.size(), &result) == nullptr);
}

TEST(Coding, Varint64Truncation) {
    uint64_t large_value = (1ull << 63) + 100ull;
    std::string s;
    put_varint64(&s, large_value);
    uint64_t result;
    for (size_t len = 0; len < s.size() - 1; len++) {
        ASSERT_TRUE(decode_varint64_ptr((const uint8_t*)s.data(), (const uint8_t*)s.data() + len, &result) == nullptr);
    }
    ASSERT_TRUE(decode_varint64_ptr((const uint8_t*)s.data(), (const uint8_t*)s.data() + s.size(), &result) != nullptr);
    ASSERT_EQ(large_value, result);
}

TEST(Coding, Strings) {
    std::string s;
    put_length_prefixed_slice(&s, Slice(""));
    put_length_prefixed_slice(&s, Slice("foo"));
    put_length_prefixed_slice(&s, Slice("bar"));
    put_length_prefixed_slice(&s, Slice(std::string(200, 'x')));

    Slice input(s);
    Slice v;
    ASSERT_TRUE(get_length_prefixed_slice(&input, &v));
    ASSERT_EQ("", v.to_string());
    ASSERT_TRUE(get_length_prefixed_slice(&input, &v));
    ASSERT_EQ("foo", v.to_string());
    ASSERT_TRUE(get_length_prefixed_slice(&input, &v));
    ASSERT_EQ("bar", v.to_string());
    ASSERT_TRUE(get_length_prefixed_slice(&input, &v));
    ASSERT_EQ(std::string(200, 'x'), v.to_string());
    ASSERT_EQ("", input.to_string());
}

}

