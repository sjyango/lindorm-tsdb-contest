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

#include "storage/tsm_writer.h"

#include <fstream>

namespace LindormContest {

    TsmWriter::TsmWriter(const std::string& vin_str, GlobalIndexManagerSPtr index_manager,
                         ThreadPoolSPtr flush_pool, Path flush_dir_path, SchemaSPtr schema)
            : _vin_str(vin_str), _index_manager(index_manager), _flush_pool(flush_pool),
            _flush_dir_path(std::move(flush_dir_path)), _schema(schema), _flush_nums(0) {
        _mem_map = std::make_unique<MemMap>(_vin_str);
    }

    TsmWriter::~TsmWriter() = default;

    void TsmWriter::append(const Row& row) {
        {
            std::lock_guard<std::mutex> l(_mutex);
            _mem_map->append(row);
        }
        flush_mem_map_sync();
    }

    void TsmWriter::flush_mem_map(MemMap *mem_map, std::string vin_str, GlobalIndexManagerSPtr index_manager,
                                  SchemaSPtr schema, Path tsm_file_path) {
        assert(mem_map != nullptr);
        TsmFile tsm_file;
        mem_map->flush_to_tsm_file(schema, tsm_file);
        // release mem map resource
        delete mem_map;
        tsm_file.write_to_file(tsm_file_path);
        // register index blocks
        index_manager->insert_indexes(vin_str, tsm_file_path.filename(), tsm_file._index_blocks);
    }

    void TsmWriter::flush_mem_map_sync(bool forced) {
        std::lock_guard<std::mutex> l(_mutex);
        if (unlikely(_mem_map->empty())) {
            return;
        }
        if (likely(!forced && !_mem_map->need_flush())) {
            return;
        }
        std::string tsm_file_name = std::to_string(_flush_nums) + "-0.tsm";
        Path tsm_file_path = _flush_dir_path / tsm_file_name;
        flush_mem_map(_mem_map.release(), _vin_str, _index_manager, _schema, tsm_file_path);
        _mem_map = std::make_unique<MemMap>(_vin_str);
        _flush_nums++;
    }

    void TsmWriter::flush_mem_map_async() {
        assert(_mem_map != nullptr);
        if (unlikely(_mem_map->empty())) {
            return;
        }
        std::string tsm_file_name = std::to_string(_flush_nums) + "-0.tsm";
        Path tsm_file_path = _flush_dir_path / tsm_file_name;
        _flush_pool->submit(flush_mem_map, _mem_map.release(), _vin_str, _index_manager, _schema, tsm_file_path);
        _mem_map = std::make_unique<MemMap>(_vin_str);
        _flush_nums++;
    }

    TsmWriterManager::TsmWriterManager(GlobalIndexManagerSPtr index_manager, ThreadPoolSPtr flush_pool, const Path &root_path)
    : _index_manager(index_manager), _flush_pool(flush_pool), _root_path(root_path) {}

    TsmWriterManager::~TsmWriterManager() = default;

    void TsmWriterManager::set_schema(SchemaSPtr schema) {
        _schema = schema;
    }

    void TsmWriterManager::append(const LindormContest::Row &row) {
        std::string vin_str(row.vin.vin, VIN_LENGTH);
        {
            std::lock_guard<SpinLock> l(_lock);
            if (unlikely(_tsm_writers.find(vin_str) == _tsm_writers.end())) {
                Path flush_dir_path = _root_path / vin_str;
                std::filesystem::create_directories(flush_dir_path);
                _tsm_writers.emplace(vin_str, std::make_unique<TsmWriter>(vin_str, _index_manager,
                                                                          _flush_pool, std::move(flush_dir_path), _schema));
            }
        }
        _tsm_writers[vin_str]->append(row);
    }

    void TsmWriterManager::flush_sync(const std::string &vin_str) {
        if (_tsm_writers.find(vin_str) == _tsm_writers.end()) {
            return;
        }
        _tsm_writers[vin_str]->flush_mem_map_sync(true);
    }

    void TsmWriterManager::flush_all_sync() {
        for (auto &tsm_writer: _tsm_writers) {
            tsm_writer.second->flush_mem_map_sync();
        }
    }

    void TsmWriterManager::flush_all_async() {
        for (auto &tsm_writer: _tsm_writers) {
            tsm_writer.second->flush_mem_map_async();
        }
    }
}