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

#include <unordered_map>

#include "Root.h"
#include "struct/Vin.h"
#include "struct/Row.h"
#include "struct/ColumnValue.h"
#include "common/coding.h"
#include "common/spinlock.h"

namespace LindormContest {

    const size_t LATEST_MANAGER_SHARD_NUM = 16;

    inline size_t hash_index(const std::string& vin_str) {
        return std::hash<std::string>()(vin_str) % LATEST_MANAGER_SHARD_NUM;
    }

    // multi thread safe
    struct LatestManager {
        std::unordered_map<std::string, SpinLock*> _latest_mutexes;
        std::unordered_map<std::string, Row> _latest_records;
        SpinLock _global_mutex;

        LatestManager() = default;

        ~LatestManager() {
            for (auto &item: _latest_mutexes) {
                delete item.second;
            }
        }

        void add_latest(const std::string& vin_str, const Row& row) {
            {
                std::lock_guard<SpinLock> l(_global_mutex);
                if (_latest_mutexes.find(vin_str) == _latest_mutexes.end()) {
                    _latest_mutexes.emplace(vin_str, new SpinLock());
                    _latest_records.emplace(vin_str, row);
                }
            }
            {
                std::lock_guard<SpinLock> l(*_latest_mutexes[vin_str]);
                if (row.timestamp > _latest_records[vin_str].timestamp) {
                    _latest_records[vin_str] = row;
                }
            }
        }

        Row get_latest(const std::string& vin_str) {
            std::lock_guard<SpinLock> l(*_latest_mutexes[vin_str]);
            return _latest_records[vin_str];
        }
    };

    struct ShardLatestManager {
        LatestManager _latest_managers[LATEST_MANAGER_SHARD_NUM];

        void add_latest(const Row& row) {
            std::string vin_str(row.vin.vin, VIN_LENGTH);
            size_t shard_idx = hash_index(vin_str);
            _latest_managers[shard_idx].add_latest(vin_str, row);
        }

        Row get_latest(const Vin& vin, const std::set<std::string>& requested_columns) {
            std::string vin_str(vin.vin, VIN_LENGTH);
            size_t shard_idx = hash_index(vin_str);
            const Row& latest_row = _latest_managers[shard_idx].get_latest(vin_str);
            Row result;
            result.vin = vin;
            result.timestamp = latest_row.timestamp;
            for (const auto& requested_column : requested_columns) {
                result.columns.emplace(requested_column, latest_row.columns.at(requested_column));
            }
            return result;
        }
    };

}