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

#include <limits>

#include "Root.h"
#include "struct/Vin.h"

namespace LindormContest {

struct RowPosition {
    size_t _segment_id;
    ordinal_t _ordinal;
    uint16_t _timestamp;
};

// LSVNV2182E0107499 -> 107499
static int32_t decode_vin(Vin vin) {
    char suffix_chars[7];
    std::memcpy(suffix_chars, vin.vin + 11, 6);
    suffix_chars[6] = '\0';
    int32_t res;
    try {
        res = std::stoi(suffix_chars);
    } catch (const std::invalid_argument& e) {
        // INFO_LOG("decode_vin throw an invalid_argument exception, input vin is %s", vin.vin)
        return -1;
    } catch (const std::out_of_range& e) {
        // INFO_LOG("decode_vin throw an out_of_range exception, input vin is %s", vin.vin)
        return -1;
    } catch (const std::exception& e) {
        // INFO_LOG("decode_vin throw an exception, input vin is %s", vin.vin)
        return -1;
    }
    return res;
}

// 107499 -> LSVNV2182E0107499
static Vin encode_vin(int32_t vin_val) {
    Vin vin;
    std::string vin_val_str = std::to_string(vin_val);
    while (vin_val_str.size() < 6) {
        vin_val_str = "0" + vin_val_str;
    }
    std::string vin_str = "LSVNV2182E0" + vin_val_str;
    std::strncpy(vin.vin, vin_str.c_str(), 17);
    return vin;
}

// 1689091499000 -> 1499
static uint16_t decode_timestamp(int64_t timestamp) {
    return (timestamp / 1000) % 10000;
}

// 1499 -> 1689091499000
static int64_t encode_timestamp(uint16_t timestamp_val) {
    return 1689090000000 + timestamp_val * 1000;
}

}