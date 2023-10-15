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

#include <atomic>

#include "index_manager.h"
#include "convert_manager.h"
#include "struct/Schema.h"
#include "common/coding.h"
#include "common/thread_pool.h"
#include "common/spinlock.h"
#include "storage/tsm_file.h"
#include "compression/compressor.h"

namespace LindormContest {

    class TsmWriter {
    public:
        TsmWriter(uint16_t vin_num, const Path& flush_dir_path, GlobalConvertManagerSPtr convert_manager)
                : _vin_num(vin_num), _convert_manager(convert_manager),
                  _flush_dir_path(flush_dir_path), _schema(nullptr) {}

        ~TsmWriter() = default;

        void init(SchemaSPtr schema) {
            _schema = schema;
            for (uint16_t file_idx = 0; file_idx < TSM_FILE_COUNT; ++file_idx) {
                Path flush_file_path = _flush_dir_path / std::to_string(file_idx);
                _streams[file_idx].open(flush_file_path, std::ios::out | std::ios::binary);
                assert(_streams[file_idx].is_open() && _streams[file_idx].good());
                _cache[file_idx].reserve(ROW_CACHE_SIZE + 1600);
                _write_nums[file_idx] = 0;
            }
        }

        void append(const Row& row) {
            uint16_t file_idx = decode_ts(row.timestamp) / FILE_CONVERT_SIZE;
            {
                std::lock_guard<std::mutex> l(_mutexes[file_idx]);
                io::serialize_row(row, false, _cache[file_idx]);
                if (unlikely(_cache[file_idx].size() >= ROW_CACHE_SIZE)) {
                    _streams[file_idx].write(_cache[file_idx].data(), _cache[file_idx].size());
                    _cache[file_idx].clear();
                    _cache[file_idx].reserve(ROW_CACHE_SIZE + 1600);
                }
                if (unlikely(++_write_nums[file_idx] == FILE_CONVERT_SIZE)) {
                    if (!_cache[file_idx].empty()) {
                        _streams[file_idx].write(_cache[file_idx].data(), _cache[file_idx].size());
                        _cache[file_idx].clear();
                        _cache[file_idx].shrink_to_fit();
                    }
                    _streams[file_idx].close();
                    _convert_manager->convert_async(_vin_num, file_idx);
                }
            }
        }

        void flush() {
            for (uint16_t file_idx = 0; file_idx < TSM_FILE_COUNT; ++file_idx) {
                std::lock_guard<std::mutex> l(_mutexes[file_idx]);
                if (!_cache[file_idx].empty()) {
                    _streams[file_idx].write(_cache[file_idx].data(), _cache[file_idx].size());
                    _cache[file_idx].clear();
                    _cache[file_idx].reserve(ROW_CACHE_SIZE + 1600);
                }
                _streams[file_idx].flush();
            }
        }

    private:
        uint16_t _vin_num;
        Path _flush_dir_path;
        SchemaSPtr _schema;
        std::string _cache[TSM_FILE_COUNT];
        uint16_t _write_nums[TSM_FILE_COUNT];
        std::mutex _mutexes[TSM_FILE_COUNT];
        std::ofstream _streams[TSM_FILE_COUNT];
        GlobalConvertManagerSPtr _convert_manager;
    };

    class TsmWriterManager;

    using TsmWriterManagerUPtr = std::unique_ptr<TsmWriterManager>;

    class TsmWriterManager {
    public:
        TsmWriterManager(const Path& root_path, GlobalConvertManagerSPtr convert_manager) {
            for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
                Path flush_dir_path = root_path / "no-compaction" / std::to_string(vin_num);
                std::filesystem::create_directories(flush_dir_path);
                _tsm_writers[vin_num] = std::make_unique<TsmWriter>(vin_num, flush_dir_path, convert_manager);
            }
        }

        ~TsmWriterManager() = default;

        void init(SchemaSPtr schema) {
            for (auto &tsm_writer: _tsm_writers) {
                tsm_writer->init(schema);
            }
        }

        void append(const Row& row) {
            _tsm_writers[decode_vin(row.vin)]->append(row);
        }

        void flush(uint16_t vin_num) {
            _tsm_writers[vin_num]->flush();
        }

    private:
        std::unique_ptr<TsmWriter> _tsm_writers[VIN_NUM_RANGE];
    };

}
