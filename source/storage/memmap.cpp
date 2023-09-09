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
#include "storage/tsm_writer.h"

namespace LindormContest {

    /// #################### MemMap #####################

    MemMap::MemMap(Path root_path, SchemaSPtr schema, std::string vin_str)
    : _root_path(root_path), _vin_str(vin_str), _schema(schema), _size(0), _flush_count(0) {}

    MemMap::~MemMap() = default;

    void MemMap::append(const Row &row) {
        std::lock_guard<std::mutex> l(_mutex);
        if (unlikely(_need_flush())) {
            flush();
        }

        for (auto& column : row.columns) {
            if (_mem_map.find(column.first) == _mem_map.end()) {
                _mem_map[column.first] = {};
            }
            _mem_map[column.first].push_back(row.timestamp, column.second);
        }

        _size++;
    }

    bool MemMap::_need_flush() const {
        return _size >= MEMMAP_FLUSH_SIZE;
    }

    void MemMap::flush() {
        Path tsm_file_path = _root_path / _vin_str / std::to_string(_flush_count);
        std::unique_ptr<MemMapWriter> mem_map_writer = std::make_unique<MemMapWriter>(tsm_file_path, _schema, *this);
        mem_map_writer->write();
        _flush_count++;
        reset();
    }

    void MemMap::reset() {
        _size = 0;
        _mem_map.clear();
    }

    const std::map<std::string, InternalValue>& MemMap::get_mem_map() const {
        return _mem_map;
    }

    /// #################### MemMap #####################

    /// ################## ShardMemMap ##################

    ShardMemMap::ShardMemMap() = default;

    ShardMemMap::~ShardMemMap() = default;

    void ShardMemMap::set_root_path(const Path &root_path) {
        _root_path = root_path;
    }

    void ShardMemMap::set_schema(SchemaSPtr schema) {
        _schema = schema;
    }

    void ShardMemMap::append(const Row &row) {
        std::string vin_str(row.vin.vin, VIN_LENGTH);
        {
            std::lock_guard<SpinLock> l(_mutex);
            if (_mem_maps.find(vin_str) == _mem_maps.end()) {
                _mem_maps.emplace(vin_str, std::make_unique<MemMap>(_root_path, _schema, vin_str));
            }
        }
        _mem_maps[vin_str]->append(row);
    }

    /// ################## ShardMemMap ##################
}