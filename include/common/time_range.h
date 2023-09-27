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

    struct TimeRange {
        int64_t _start_time; // inclusive
        int64_t _end_time;   // exclusive

        TimeRange() = default;

        TimeRange(int64_t start_time, int64_t end_time) : _start_time(start_time), _end_time(end_time) {}

        bool overlap(const TimeRange& other) const {
            return _start_time < other._end_time && other._start_time < _end_time;
        }

        int64_t interval_nums(int64_t interval) const {
            assert((_end_time - _start_time) % interval == 0);
            return (_end_time - _start_time) / interval;
        }

        TimeRange sub_interval(int64_t interval, int64_t index) const {
            assert(index < interval_nums(interval));
            return {_start_time + index * interval, _start_time + (index + 1) * interval};
        }

        std::vector<TimeRange> sub_intervals(int64_t interval) const {
            std::vector<TimeRange> trs;
            int64_t interval_count = interval_nums(interval);
            for (int64_t i = 0; i < interval_count; ++i) {
                trs.emplace_back(sub_interval(interval, i));
            }
            return trs;
        }
    };

    struct IndexRange {
        uint16_t _start_index;  // inclusive
        uint16_t _end_index;    // exclusive
        uint16_t _block_index;

        IndexRange(uint16_t start_index, uint16_t end_index, uint16_t block_index)
        : _start_index(start_index), _end_index(end_index), _block_index(block_index) {}

        uint32_t global_start_index() const {
            return (uint32_t) _block_index * DATA_BLOCK_ITEM_NUMS + _start_index;
        }

        uint32_t global_end_index() const {
            return (uint32_t) _block_index * DATA_BLOCK_ITEM_NUMS + _end_index;
        }
    };

}