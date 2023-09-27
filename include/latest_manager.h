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

        void set_schema(SchemaSPtr schema) {
            _schema = schema;
        }

        bool query_latest(uint16_t vin_num, const std::set<std::string>& requested_columns, Row &result_row) {
            if (_finish_compaction) {
                return get_latest(vin_num, requested_columns, result_row);
            } else {
                Path vin_dir_path = _root_path / "no-compaction" / std::to_string(vin_num);
                if (!std::filesystem::exists(vin_dir_path)) {
                    return false;
                }
                std::vector<Path> file_paths;
                _get_file_paths(vin_dir_path, file_paths);
                if (file_paths.empty()) {
                    return false;
                }
                Row latest_row;

                for (const auto& file_path: file_paths) {
                    query_from_one_flush_file(file_path, latest_row);
                }

                result_row.timestamp = latest_row.timestamp;

                for (const auto& requested_column : requested_columns) {
                    result_row.columns.emplace(requested_column, latest_row.columns.at(requested_column));
                }

                return true;
            }
        }

        bool get_latest(uint16_t vin_num, const std::set<std::string>& requested_columns, Row &result_row) {
            Row latest_row = _latest_records[vin_num];
            result_row.timestamp = latest_row.timestamp;
            for (const auto& requested_column : requested_columns) {
                result_row.columns.emplace(requested_column, latest_row.columns.at(requested_column));
            }
            return true;
        }

        void query_from_one_flush_file(const Path& flush_file_path, Row &latest_row) {
            std::ifstream input_file;
            input_file.open(flush_file_path, std::ios::in | std::ios::binary);
            if (!input_file.is_open() || !input_file.good()) {
                INFO_LOG("%s open failed", flush_file_path.c_str())
                throw std::runtime_error("time range open file failed");
            }

            while (!input_file.eof()) {
                Row row;
                if (!io::read_row_from_file(input_file, _schema, false, row)) {
                    break;
                }
                if (row.timestamp > latest_row.timestamp) {
                    latest_row = row;
                }
            }

            input_file.close();
        }

        void _get_file_paths(const Path& vin_dir_path, std::vector<Path>& file_paths) {
            for (const auto& entry: std::filesystem::directory_iterator(vin_dir_path)) {
                if (entry.is_regular_file()) {
                    file_paths.emplace_back(entry.path());
                }
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