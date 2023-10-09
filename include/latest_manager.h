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
#include <mutex>
#include <shared_mutex>
#include <fstream>
#include <optional>

#include "struct/Vin.h"
#include "struct/Row.h"
#include "struct/ColumnValue.h"
#include "common/coding.h"
#include "common/spinlock.h"
#include "io/io_utils.h"

namespace LindormContest {

    class GlobalLatestManager;

    using GlobalLatestManagerUPtr = std::unique_ptr<GlobalLatestManager>;

    class GlobalLatestManager {
    public:
        GlobalLatestManager(const Path& root_path, bool finish_compaction)
        : _root_path(root_path), _finish_compaction(finish_compaction) {}

        ~GlobalLatestManager() = default;

        void init(SchemaSPtr schema) {
            _schema = schema;
        }

        void query_latest(uint16_t vin_num, const std::set<std::string>& requested_columns, Row &result_row) {
            result_row.timestamp = _latest_records[vin_num].timestamp;
            for (const auto& requested_column : requested_columns) {
                result_row.columns.emplace(requested_column, _latest_records[vin_num].columns[requested_column]);
            }
        }

        void query_latest(uint16_t vin_num, uint16_t latest_ts, const std::set<std::string>& requested_columns, Row &result_row) {
            result_row.timestamp = encode_ts(latest_ts);
            uint16_t file_idx = latest_ts / FILE_CONVERT_SIZE;
            uint16_t file_offset = latest_ts % FILE_CONVERT_SIZE;
            Path flush_dir_path = _root_path / "no-compaction" / std::to_string(vin_num) / std::to_string(file_idx);

            for (const auto &requested_column: requested_columns) {
                Path flush_file_path = flush_dir_path / requested_column;
                int fd = open(flush_file_path.c_str(), O_RDONLY);
                assert(fd != -1);

                switch (_schema->columnTypeMap[requested_column]) {
                    case COLUMN_TYPE_INTEGER: {
                        auto res = lseek(fd, file_offset * sizeof(int32_t), SEEK_SET);
                        assert(res != -1);
                        int32_t int_value;
                        auto bytes_read = read(fd, &int_value, sizeof(int32_t));
                        assert(bytes_read == sizeof(int32_t));
                        result_row.columns.emplace(requested_column, int_value);
                        break;
                    }
                    case COLUMN_TYPE_DOUBLE_FLOAT: {
                        auto res = lseek(fd, file_offset * sizeof(double_t), SEEK_SET);
                        assert(res != -1);
                        double_t double_value;
                        auto bytes_read = read(fd, &double_value, sizeof(double_t));
                        assert(bytes_read == sizeof(double_t));
                        result_row.columns.emplace(requested_column, double_value);
                        break;
                    }
                    case COLUMN_TYPE_STRING: {
                        auto file_size = lseek(fd, 0, SEEK_END);
                        assert(file_size != -1);
                        std::string str_buf;
                        str_buf.resize(file_size);
                        auto res = lseek(fd, 0, SEEK_SET);
                        assert(res != -1);
                        auto bytes_read = read(fd, str_buf.data(), file_size);
                        assert(bytes_read == file_size);
                        size_t str_offset = 0;

                        while (str_offset != file_size) {
                            uint16_t str_idx = *reinterpret_cast<uint16_t*>(str_buf.data() + str_offset);
                            str_offset += sizeof(uint16_t);
                            assert(str_idx < FILE_CONVERT_SIZE);
                            int32_t str_length = *reinterpret_cast<int32_t*>(str_buf.data() + str_offset);
                            str_offset += sizeof(int32_t);
                            if (unlikely(str_idx == file_offset)) {
                                result_row.columns.emplace(requested_column, ColumnValue(str_buf.data() + str_offset, str_length));
                                break;
                            }
                            str_offset += str_length;
                        }
                        break;
                    }
                    default:
                        break;
                }

                close(fd);
            }
        }

        void load_latest_records_from_file(const Path& latest_records_path) {
            if (!std::filesystem::exists(latest_records_path)) {
                INFO_LOG("latest_records file doesn't exist")
                return;
            }
            std::ifstream input_file(latest_records_path, std::ios::in | std::ios::binary);
            if (!input_file.is_open()) {
                INFO_LOG("latest_records file doesn't exist")
                return;
            }

            for (uint16_t i = 0; i < VIN_NUM_RANGE; ++i) {
                Row row;
                io::read_row_from_file(input_file, _schema, true, row);
                uint16_t vin_num = decode_vin(row.vin);
                _latest_records[vin_num] = row;
            }

            input_file.close();
        }

    private:
        Path _root_path;
        bool _finish_compaction;
        SchemaSPtr _schema;
        Row _latest_records[VIN_NUM_RANGE];
    };
}