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
#include "storage/memmap_writer.h"

namespace LindormContest {

    inline size_t hash_index(const std::string& vin_str) {
        return std::hash<std::string>()(vin_str) % MEMMAP_SHARD_NUM;
    }

    MemMap::MemMap() : _size(0), _shard_idx(0), _flush_count(0) {}

    MemMap::~MemMap() = default;

    void MemMap::set_shard_idx(uint8_t shard_idx) {
        _shard_idx = shard_idx;
    }

    void MemMap::set_root_path(const Path &root_path) {
        _root_path = root_path;
    }

    void MemMap::set_schema(SchemaSPtr schema) {
        _schema = schema;
    }

    void MemMap::append(const Row &row) {
        std::lock_guard<std::mutex> l(_mutex);
        if (unlikely(_need_flush())) {
            flush();
        }

        for (auto& column : row.columns) {
            InternalKey key {row.vin, column.first};
            if (_mem_map.find(key) == _mem_map.end()) {
                _mem_map[key] = {};
            }
            _mem_map[key].push_back(row.timestamp, column.second);
        }

        _size++;
    }

    bool MemMap::_need_flush() const {
        return _size >= MEMMAP_FLUSH_SIZE;
    }

    void MemMap::flush() {
        Path tsm_file_path = _root_path / std::to_string(_shard_idx) / std::to_string(_flush_count);
        std::unique_ptr<MemMapWriter> mem_map_writer = std::make_unique<MemMapWriter>(tsm_file_path, _schema, *this);
        mem_map_writer->write();
        _flush_count++;
        reset();
    }

    void MemMap::reset() {
        _size = 0;
        _mem_map.clear();
    }

    const std::map<InternalKey, InternalValue> &MemMap::get_mem_map() const {
        return _mem_map;
    }

    ShardMemMap::ShardMemMap() = default;

    ShardMemMap::~ShardMemMap() = default;

    void ShardMemMap::set_root_path(const Path &root_path) {
        for (auto &mem_map: _mem_maps) {
            mem_map.set_root_path(root_path);
        }
    }

    void ShardMemMap::set_schema(SchemaSPtr schema) {
        for (auto &mem_map: _mem_maps) {
            mem_map.set_schema(schema);
        }
    }

    void ShardMemMap::append(const Row &row) {
        size_t shard_idx = hash_index(std::string(row.vin.vin + 12, 5));
        _mem_maps[shard_idx].append(row);
    }

}