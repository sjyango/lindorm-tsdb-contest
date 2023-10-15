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
        GlobalLatestManager(const Path& root_path, bool finish_compaction) : _root_path(root_path) {}

        ~GlobalLatestManager() = default;

        void init(SchemaSPtr schema) {
            _schema = schema;
        }

        template <bool finish_compaction>
        void query_latest(uint16_t vin_num, const std::set<std::string>& requested_columns, Row &result_row) {
            if constexpr (finish_compaction) {
                result_row.timestamp = _latest_records[vin_num].timestamp;

                for (const auto& requested_column : requested_columns) {
                    result_row.columns[requested_column] = _latest_records[vin_num].columns.at(requested_column);
                }
            } else {
                Row latest_row;

                for (uint16_t file_idx = 0; file_idx < TSM_FILE_COUNT; ++file_idx) {
                    Path flush_file_path = _root_path / "no-compaction" / std::to_string(vin_num) / std::to_string(file_idx);
                    query_from_one_flush_file(flush_file_path, latest_row);
                }

                result_row.timestamp = latest_row.timestamp;

                for (const auto& requested_column : requested_columns) {
                    result_row.columns.emplace(requested_column, latest_row.columns.at(requested_column));
                }
            }
        }

        void query_from_one_flush_file(const Path& flush_file_path, Row &latest_row) {
            std::string buf;
            io::stream_read_string_from_file(flush_file_path, buf);

            if (buf.empty()) {
                return;
            }

            const char* start = buf.c_str();
            const char* end = start + buf.size();

            while (start < end) {
                Row row;
                io::deserialize_row(_schema, start, false, row);
                if (row.timestamp > latest_row.timestamp) {
                    latest_row = row;
                }
            }
        }

        void set_latest_row(uint16_t vin_num, const Row& latest_row) {
            _latest_records[vin_num] = latest_row;
        }

        // void load_latest_records_from_file(const Path& latest_records_path) {
        //     if (!std::filesystem::exists(latest_records_path)) {
        //         INFO_LOG("latest_records file doesn't exist")
        //         return;
        //     }
        //     std::ifstream input_file(latest_records_path, std::ios::in | std::ios::binary);
        //     if (!input_file.is_open()) {
        //         INFO_LOG("latest_records file doesn't exist")
        //         return;
        //     }
        //     std::string buf;
        //     input_file.seekg(0, std::ios::end);
        //     auto file_size = input_file.tellg();
        //     input_file.seekg(0, std::ios::beg);
        //     buf.resize(file_size);
        //     input_file.read(buf.data(), file_size);
        //     const char* p = buf.c_str();
        //
        //     for (uint16_t i = 0; i < VIN_NUM_RANGE; ++i) {
        //         Row row;
        //         io::deserialize_row(_schema, p, true, row);
        //         uint16_t vin_num = decode_vin(row.vin);
        //         _latest_records[vin_num] = row;
        //     }
        //
        //     input_file.close();
        // }

    private:
        Path _root_path;
        SchemaSPtr _schema;
        Row _latest_records[VIN_NUM_RANGE];
    };
}