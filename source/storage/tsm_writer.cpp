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

    TsmWriter::TsmWriter(ThreadPoolSPtr flush_pool, const Path &flush_dir_path,
                         std::string vin_str, SchemaSPtr schema)
            : _flush_pool(flush_pool), _flush_dir_path(flush_dir_path),
              _vin_str(std::move(vin_str)), _schema(schema), _flush_nums(0) {
        _mem_map = std::make_unique<MemMap>();
    }

    TsmWriter::~TsmWriter() = default;

    void TsmWriter::append(const Row &row) {
        std::lock_guard<std::mutex> l(_mutex);
        _mem_map->append(row);
        if (unlikely(_mem_map->need_flush())) {
            flush_mem_map_async();
            reset_mem_map();
        }
    }

    void TsmWriter::flush_mem_map(MemMap *mem_map, SchemaSPtr schema, Path tsm_file_path) {
        assert(mem_map != nullptr);
        TsmFile tsm_file;
        mem_map->flush_to_tsm_file(schema, tsm_file);
        // release mem map resource
        delete mem_map;
        tsm_file.write_to_file(tsm_file_path);
    }

    void TsmWriter::flush_mem_map_async() {
        assert(_mem_map != nullptr);
        std::string tsm_file_name = std::to_string(_flush_nums) + "-0.tsm";
        Path tsm_file_path = _flush_dir_path / tsm_file_name;
        _flush_pool->submit(flush_mem_map, _mem_map.release(), _schema, tsm_file_path);
    }

    void TsmWriter::reset_mem_map() {
        _mem_map = std::make_unique<MemMap>();
        _flush_nums++;
    }
}