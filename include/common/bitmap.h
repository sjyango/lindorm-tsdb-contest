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

class BitMap {
    static constexpr UInt32 BIT_NUMS = 8 * sizeof(UInt32);

public:
    BitMap(size_t size) : _size(size), _bits((size + BIT_NUMS - 1) / BIT_NUMS, 0) {}

    void set(size_t pos) {
        size_t index = pos / BIT_NUMS;
        size_t offset = pos % BIT_NUMS;
        _bits[index] |= (1 << offset);
    }

    bool get(int pos) const {
        size_t index = pos / BIT_NUMS;
        size_t offset = pos % BIT_NUMS;
        return (_bits[index] & (1 << offset)) != 0;
    }

    void add_range(size_t start, size_t end) {
        for (size_t i = start; i < end; ++i) {
            set(i);
        }
    }

    void intersect(const BitMap& other) {
        for (size_t i = 0; i < _bits.size(); ++i) {
            _bits[i] &= other._bits[i];
        }
    }

    String print() const {
        String res;
        for (int i = 0; i < _size; i++) {
            res += std::to_string(get(i));
        }
        return res;
    }

private:
    size_t _size;
    std::vector<UInt32> _bits;
};

}