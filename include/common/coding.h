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

#include "Root.h"

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
}