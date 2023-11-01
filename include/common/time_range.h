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
        uint16_t _start_idx; // inclusive
        uint16_t _end_idx;   // inclusive

        TimeRange() = default;

        TimeRange(uint16_t start_idx, uint16_t end_idx) : _start_idx(start_idx), _end_idx(end_idx) {}

        void init(int64_t start_time_inclusive, int64_t end_time_exclusive) {
            if (start_time_inclusive % 1000 == 0) {
                _start_idx = decode_ts(start_time_inclusive);
            } else {
                _start_idx = decode_ts(start_time_inclusive) + 1;
            }
            if (end_time_exclusive % 1000 == 0) {
                _end_idx = decode_ts(end_time_exclusive) - 1;
            } else {
                _end_idx = decode_ts(end_time_exclusive);
            }
        }

        bool overlap(const TimeRange& other) const {
            return _start_idx <= other._start_idx && other._end_idx <= _end_idx;
        }

        uint32_t interval_nums(uint32_t interval) const {
            assert((_end_idx - _start_idx + 1) % interval == 0);
            return (_end_idx - _start_idx + 1) / interval;
        }

        TimeRange sub_interval(uint32_t interval, uint32_t index) const {
            assert(index < interval_nums(interval));
            return {static_cast<uint16_t>(_start_idx + index * interval),
                    static_cast<uint16_t>(_start_idx + (index + 1) * interval - 1)};
        }

        std::vector<TimeRange> sub_intervals(int64_t interval) const {
            std::vector<TimeRange> trs;
            uint32_t interval_count = interval_nums(interval / 1000);
            for (uint32_t i = 0; i < interval_count; ++i) {
                trs.emplace_back(sub_interval(interval / 1000, i));
            }
            return trs;
        }

        uint16_t range_width() const {
            return _end_idx - _start_idx + 1;
        }
    };

    struct IndexRange {
        uint16_t _start_index;  // inclusive
        uint16_t _end_index;    // inclusive

        IndexRange(uint16_t start_index, uint16_t end_index) : _start_index(start_index), _end_index(end_index) {}

        ~IndexRange() = default;
    };

}