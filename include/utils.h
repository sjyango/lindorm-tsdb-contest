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

// increase the last position char
static std::string increase_vin(Vin vin) {
    vin.vin[16] += 1;
    return {vin.vin, 17};
}

static std::string decrease_vin(Vin vin) {
    vin.vin[16] -= 1;
    return {vin.vin, 17};
}

struct RowPosition {
    size_t _segment_id;
    std::string _vin;
    int64_t _timestamp;
    ordinal_t _ordinal;

    RowPosition() = default;

    RowPosition(size_t segment_id, std::string vin, int64_t timestamp, ordinal_t ordinal)
            : _segment_id(segment_id), _vin(vin), _timestamp(timestamp), _ordinal(ordinal) {}

    ~RowPosition() = default;

    bool operator==(const RowPosition& other) const {
        return _vin == other._vin && _timestamp == other._timestamp;
    }

    bool operator!=(const RowPosition& other) const {
        return !(*this == other);
    }

    struct HashFunc {
        size_t operator()(const RowPosition& row) const {
            return std::hash<std::string>()(row._vin) ^ std::hash<int64_t>()(row._timestamp);
        }
    };
};

}