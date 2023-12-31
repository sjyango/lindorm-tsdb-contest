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

#pragma once

#include <filesystem>
#include <cassert>

#include "Root.h"
#include "struct/Schema.h"
#include "struct/Vin.h"
#include "struct/Row.h"

namespace LindormContest {

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

    // DEBUG:   3981 ~ 8981 LSVNV2182E024
    // RELEASE: 1654 ~ 6654 LSVNV2182E054

    static const std::string VIN_PREFIX = "LSVNV2182E054";
    static constexpr uint16_t VIN_NUM_RANGE = 5000;
    static constexpr uint16_t MIN_VIN_NUM = 1654;
    static constexpr uint16_t MAX_VIN_NUM = MIN_VIN_NUM + VIN_NUM_RANGE;
    static constexpr uint16_t INVALID_VIN_NUM = std::numeric_limits<uint16_t>::max();
    static constexpr int64_t MIN_TS = 1694043124000;
    static constexpr int64_t MAX_TS = 1694079123000;
    static constexpr uint16_t TS_NUM_RANGE = 36000;

    // 0 ~ 4999
    inline uint16_t decode_vin(const Vin& vin) {
        uint16_t vin_num = 0;
        for (uint8_t i = 0; i < 4; ++i) {
            if (!std::isdigit(vin.vin[13 + i])) {
                return INVALID_VIN_NUM;
            }
            vin_num = vin_num * 10 + (vin.vin[13 + i] - '0');
        }
        if (likely(vin_num >= MIN_VIN_NUM && vin_num < MAX_VIN_NUM)) {
            return vin_num - MIN_VIN_NUM;
        } else {
            return INVALID_VIN_NUM;
        }
    }

    inline Vin encode_vin(uint16_t vin_num) {
        assert(vin_num >= 0 && vin_num < VIN_NUM_RANGE);
        std::string vin_str = VIN_PREFIX + std::to_string(vin_num + MIN_VIN_NUM);
        Vin vin;
        std::memcpy(vin.vin, vin_str.c_str(), VIN_LENGTH);
        return vin;
    }

    using Path = std::filesystem::path;
    using SchemaSPtr = std::shared_ptr<Schema>;

    inline uint16_t decode_ts(int64_t ts) {
        return (ts - MIN_TS) / 1000;
    }

    inline int64_t encode_ts(uint16_t ts) {
        return ts * 1000 + MIN_TS;
    }

    inline void swap_row(Row& lhs, Row& rhs) {
        std::swap(lhs.vin, rhs.vin);
        std::swap(lhs.timestamp, rhs.timestamp);
        std::swap(lhs.columns, rhs.columns);
    }

    inline uint32_t get_next_power_of_two(uint32_t n) {
        return std::ceil(std::log2(n));
    }

    static constexpr uint16_t SCHEMA_COLUMN_NUMS = 60;
    static constexpr uint16_t DATA_BLOCK_ITEM_NUMS = 2000;
    static constexpr uint16_t FILE_CONVERT_SIZE = 36000;
    static constexpr uint16_t TSM_FILE_COUNT = TS_NUM_RANGE / FILE_CONVERT_SIZE;
    static constexpr uint16_t DATA_BLOCK_COUNT = FILE_CONVERT_SIZE / DATA_BLOCK_ITEM_NUMS;
    static constexpr uint16_t POOL_THREAD_NUM = 8;
    static constexpr uint32_t BITPACKING_RANGE_NUM = 1 << 6;
    static constexpr uint32_t ROW_CACHE_SIZE = 256 * 1024;

    static_assert(TS_NUM_RANGE % FILE_CONVERT_SIZE == 0);
    static_assert(FILE_CONVERT_SIZE % DATA_BLOCK_ITEM_NUMS == 0);

    static const int64_t LONG_DOUBLE_NAN = 0xfff0000000000000L;
    static const double_t DOUBLE_NAN = *(double_t*)(&LONG_DOUBLE_NAN);
    static const int32_t INT_NAN = 0x80000000;
    static const double_t EPSILON = std::pow(10.0, -5);

    static constexpr int BLOCK_HEADER_SIZE = sizeof(uint64_t);
    static constexpr int BLOCK_ALLOC_SIZE = DATA_BLOCK_ITEM_NUMS * sizeof(double_t) * 2;
    static constexpr int BLOCK_SIZE = BLOCK_ALLOC_SIZE - BLOCK_HEADER_SIZE;

#define ERR_LOG(str, ...) {                                  \
    fprintf(stderr, "%s:%d. [ERROR]: ", __FILE__, __LINE__); \
    fprintf(stderr, str, ##__VA_ARGS__);                     \
    fprintf(stderr, "\n");                                   \
}

#define INFO_LOG(str, ...) {                                 \
    fprintf(stdout, "%s:%d. [INFO]: ", __FILE__, __LINE__);  \
    fprintf(stdout, str, ##__VA_ARGS__);                     \
    fprintf(stdout, "\n");                                   \
}

#define RECORD_TIME_COST(name, code)                                                                             \
    do {                                                                                                         \
        auto start_##name = std::chrono::high_resolution_clock::now();                                           \
        code                                                                                                     \
        auto end_##name = std::chrono::high_resolution_clock::now();                                             \
        auto duration_##name = std::chrono::duration_cast<std::chrono::milliseconds>(end_##name - start_##name); \
        INFO_LOG("time cost for %s: %ld ms", #name, duration_##name.count())                                     \
    } while (false);
}

#define STANDARD_VECTOR_SIZE 2048