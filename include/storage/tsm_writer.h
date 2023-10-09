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
                std::filesystem::create_directories(_flush_dir_path / std::to_string(file_idx));
                for (const auto &[column_name, column_type]: _schema->columnTypeMap) {
                    Path column_file_path = _flush_dir_path / std::to_string(file_idx) / column_name;
                    int fd = open(column_file_path.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
                    assert(fd != -1);
                    _fds[file_idx][column_name] = fd;
                    _mutexes[file_idx][column_name] = std::make_unique<std::mutex>();
                    if (column_type == COLUMN_TYPE_INTEGER) {
                        auto res = ftruncate(fd, FILE_CONVERT_SIZE * sizeof(int32_t));
                        assert(res != -1);
                    } else if (column_type == COLUMN_TYPE_DOUBLE_FLOAT) {
                        auto res = ftruncate(fd, FILE_CONVERT_SIZE * sizeof(double_t));
                        assert(res != -1);
                    }
                }
            }
        }

        void append(const Row& row) {
            uint16_t ts_num = decode_ts(row.timestamp);
            if (ts_num > _latest_ts.load(std::memory_order::relaxed)) {
                _latest_ts.store(ts_num, std::memory_order::relaxed);
            }
            uint16_t file_idx = ts_num / FILE_CONVERT_SIZE;
            uint16_t file_offset = ts_num % FILE_CONVERT_SIZE;

            for (const auto &[column_name, column_value]: row.columns) {
                std::lock_guard<std::mutex> l(*_mutexes[file_idx][column_name]);
                int fd = _fds[file_idx][column_name];

                switch (column_value.columnType) {
                    case COLUMN_TYPE_INTEGER: {
                        auto res = lseek(fd, file_offset * sizeof(int32_t), SEEK_SET);
                        assert(res != -1);
                        auto bytes_written = write(fd, column_value.columnData, sizeof(int32_t));
                        assert(bytes_written != -1);
                        break;
                    }
                    case COLUMN_TYPE_DOUBLE_FLOAT: {
                        auto res = lseek(fd, file_offset * sizeof(double_t), SEEK_SET);
                        assert(res != -1);
                        auto bytes_written = write(fd, column_value.columnData, sizeof(double_t));
                        assert(bytes_written != -1);
                        break;
                    }
                    case COLUMN_TYPE_STRING: {
                        std::string str_buf;
                        put_fixed(&str_buf, file_offset);
                        str_buf.append(column_value.columnData, column_value.getRawDataSize());
                        auto bytes_written = write(fd, str_buf.data(), str_buf.size());
                        assert(bytes_written != -1);
                        break;
                    }
                    default:
                        break;
                }
            }

            if (++_write_nums[file_idx] == FILE_CONVERT_SIZE) {
                for (const auto &item: _fds[file_idx]) {
                    close(item.second);
                }
                _convert_manager->convert_async(_vin_num, file_idx);
                INFO_LOG("%hu-%hu starts converting", _vin_num, file_idx)
            }
        }

        uint16_t get_latest_ts() const {
            return _latest_ts.load(std::memory_order::relaxed);
        }

    private:
        uint16_t _vin_num;
        std::atomic<uint16_t> _latest_ts {0};
        Path _flush_dir_path;
        SchemaSPtr _schema;
        std::atomic<uint16_t> _write_nums[TSM_FILE_COUNT] = {0};
        std::unordered_map<std::string, std::unique_ptr<std::mutex>> _mutexes[TSM_FILE_COUNT];
        std::unordered_map<std::string, int> _fds[TSM_FILE_COUNT];
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

        uint16_t get_latest_ts(uint16_t vin_num) const {
            return _tsm_writers[vin_num]->get_latest_ts();
        }

    private:
        std::unique_ptr<TsmWriter> _tsm_writers[VIN_NUM_RANGE];
    };

}
