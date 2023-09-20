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

    TsmWriter::TsmWriter(uint16_t vin_num, GlobalIndexManagerSPtr index_manager,
                         GlobalCompactionManagerSPtr compaction_manager, Path flush_dir_path)
            : _vin_num(vin_num), _index_manager(index_manager), _compaction_manager(compaction_manager),
            _flush_dir_path(std::move(flush_dir_path)), _schema(nullptr), _flush_nums(0), _compaction_nums(0) {
        _mem_map = std::make_unique<MemMap>();
    }

    TsmWriter::~TsmWriter() = default;

    void TsmWriter::set_schema(SchemaSPtr schema) {
        _schema = schema;
    }

    void TsmWriter::append(const Row& row) {
        {
            std::lock_guard<std::mutex> l(_mutex);
            _mem_map->append(row);
        }
        flush_mem_map_sync();
    }

    void TsmWriter::flush_mem_map(MemMap *mem_map, uint16_t vin_num, GlobalIndexManagerSPtr index_manager,
                                  SchemaSPtr schema, Path tsm_file_path) {
        assert(mem_map != nullptr);
        TsmFile tsm_file;
        mem_map->flush_to_tsm_file(schema, tsm_file);
        // release mem map resource
        delete mem_map;
        tsm_file.write_to_file(tsm_file_path);
        // register index blocks
        index_manager->insert_indexes(vin_num, tsm_file_path.filename(), tsm_file._index_blocks);
    }

    void TsmWriter::flush_mem_map_sync(bool forced) {
        std::lock_guard<std::mutex> l(_mutex);
        if (unlikely(_mem_map->empty())) {
            return;
        }
        if (likely(!forced && !_mem_map->need_flush())) {
            return;
        }
        std::string tsm_file_name = std::to_string(_flush_nums++) + "-0.tsm";
        Path tsm_file_path = _flush_dir_path / tsm_file_name;
        flush_mem_map(_mem_map.release(), _vin_num, _index_manager, _schema, tsm_file_path);
        _mem_map = std::make_unique<MemMap>();
        if (_flush_nums % COMPACTION_FILE_NUM == 0) {
            _compaction_manager->level_compaction_async(_vin_num, _compaction_nums, _flush_nums);
            _compaction_nums = _flush_nums;
        }
    }

    void TsmWriter::finalize_flush_mem_map_sync() {
        if (!_mem_map->empty()) {
            std::string tsm_file_name = std::to_string(_flush_nums++) + "-0.tsm";
            Path tsm_file_path = _flush_dir_path / tsm_file_name;
            flush_mem_map(_mem_map.release(), _vin_num, _index_manager, _schema, tsm_file_path);
        }
        _compaction_manager->level_compaction_async(_vin_num, _compaction_nums, _flush_nums);
    }

    TsmWriterManager::TsmWriterManager(GlobalIndexManagerSPtr index_manager, bool finish_compaction,
                                       GlobalCompactionManagerSPtr compaction_manager, const Path &root_path) {
        if (!finish_compaction) {
            for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
                Path flush_dir_path = root_path / "no-compaction" / std::to_string(vin_num);
                std::filesystem::create_directories(flush_dir_path);
                _tsm_writers[vin_num] = std::make_unique<TsmWriter>(vin_num, index_manager, compaction_manager, flush_dir_path);
            }
        }
    }

    TsmWriterManager::~TsmWriterManager() = default;

    void TsmWriterManager::set_schema(SchemaSPtr schema) {
        for (auto &tsm_writer: _tsm_writers) {
            tsm_writer->set_schema(schema);
        }
    }

    void TsmWriterManager::append(const LindormContest::Row &row) {
        uint16_t vin_num = decode_vin(row.vin);
        _tsm_writers[vin_num]->append(row);
    }

    void TsmWriterManager::force_flush_sync(uint16_t vin_num) {
        _tsm_writers[vin_num]->flush_mem_map_sync(true);
    }

    void TsmWriterManager::finalize_flush_all_sync() {
        for (auto &tsm_writer: _tsm_writers) {
            tsm_writer->finalize_flush_mem_map_sync();
        }
    }

}