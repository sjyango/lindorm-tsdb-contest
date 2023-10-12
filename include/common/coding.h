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

#include "base.h"

namespace LindormContest {

    template <typename T>
    inline void encode_fixed(uint8_t *buf, T val) {
        std::memcpy(buf, &val, sizeof(T));
    }

    template <typename T>
    inline T decode_fixed(const uint8_t*& buf) {
        T res;
        std::memcpy(&res, buf, sizeof(T));
        buf += sizeof(T);
        return res;
    }

    template <typename T, typename V>
    inline void put_fixed(T* dst, V val) {
        uint8_t buf[sizeof(val)];
        encode_fixed(buf, val);
        dst->append((char*)buf, sizeof(buf));
    }

    static uint32_t bit_packing_encoding(uint8_t required_bits, int32_t int_min, const int32_t* uncompress_data, size_t uncompress_size, char* compress_data) {
        uint32_t compress_size = 0;
        uint16_t current_byte = 0;
        uint8_t bits_in_current_byte = 0;

        for (size_t i = 0; i < uncompress_size; ++i) {
            uint16_t delta_value = uncompress_data[i] - int_min;
            current_byte |= (delta_value << bits_in_current_byte);
            bits_in_current_byte += required_bits;

            if (bits_in_current_byte >= 8) {
                compress_data[compress_size++] = current_byte & 0xFF;
                current_byte >>= 8;
                bits_in_current_byte -= 8;
            }
        }

        if (bits_in_current_byte > 0) {
            compress_data[compress_size++] = current_byte & 0xFF;
        }

        return compress_size;
    }

    static void bit_packing_decoding(uint8_t required_bits, int32_t int_min, const char* compress_data, int32_t* uncompress_data, size_t uncompress_size) {
        uint8_t MASK = (1 << required_bits) - 1;
        size_t uncompress_count = 0;
        size_t current_index = 0;
        int8_t current_bit = 0;
        uint16_t current_byte = *reinterpret_cast<const uint16_t*>(compress_data + current_index++);

        while (uncompress_count < uncompress_size) {
            uint8_t compress_value = (current_byte >> current_bit) & MASK;
            uncompress_data[uncompress_count++] = int_min + compress_value;
            current_bit += required_bits;

            if (current_bit >= 8) {
                current_byte = *reinterpret_cast<const uint16_t*>(compress_data + current_index++);
                current_bit -= 8;
            }
        }
    }
}