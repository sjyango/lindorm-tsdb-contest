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

#include "storage/memmap.h"

namespace LindormContest::storage {

    inline size_t hash_index(const std::string& vin_str) {
        return std::hash<std::string>()(vin_str) % MEMMAP_SHARD_NUM;
    }

    MemMap::MemMap() : _size(0) {}

    MemMap::~MemMap() = default;

    void MemMap::append(Row &row) {
        if (_need_flush()) {
            flush();
        }

        for (auto& column : row.columns) {
            InternalKey key {row.vin, column.first};
            if (_mem_map.find(key) == _mem_map.end()) {
                _mem_map[key] = {};
            }
            _mem_map[key].push_back(row.timestamp, std::move(column.second));
        }

        _size++;
    }

    bool MemMap::_need_flush() const {
        return _size >= MEMMAP_FLUSH_SIZE;
    }

    void MemMap::flush() {

    }

    void ShardMemMap::append(std::vector<Row> &rows) {
        for (auto& row : rows) {
            size_t shard_idx = hash_index(std::string(row.vin.vin + 12, 5));
            std::lock_guard<std::mutex> l(_mutexes[shard_idx]);
            _mem_maps[shard_idx].append(row);
        }
    }

}