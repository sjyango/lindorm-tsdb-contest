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

namespace LindormContest {

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

    static const std::string VIN_PREFIX = "LSVNV2182E024";
    static constexpr uint16_t VIN_NUM_RANGE = 5000;
    static constexpr uint16_t MIN_VIN_NUM = 3981;
    static constexpr uint16_t MAX_VIN_NUM = 8980;
    static constexpr uint16_t INVALID_VIN_NUM = 5000;

    // 0 ~ 4999
    inline uint16_t decode_vin(const Vin& vin) {
        uint16_t vin_num = 0;
        for (uint8_t i = 0; i < 4; ++i) {
            if (!std::isdigit(vin.vin[13 + i])) {
                return INVALID_VIN_NUM;
            }
            vin_num = vin_num * 10 + (vin.vin[13 + i] - '0');
        }
        if (likely(vin_num >= MIN_VIN_NUM && vin_num <= MAX_VIN_NUM)) {
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

    static constexpr size_t SCHEMA_COLUMN_NUMS = 60;
    static constexpr uint16_t DATA_BLOCK_ITEM_NUMS = 180;
    static constexpr size_t MEMMAP_FLUSH_SIZE = 180;

    static const int64_t LONG_DOUBLE_NAN = 0xfff0000000000000L;
    static const double_t DOUBLE_NAN = *(double_t*)(&LONG_DOUBLE_NAN);
    static const int32_t INT_NAN = 0x80000000;
    static const double_t EPSILON = std::pow(10.0, -5);



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
