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

    // multi thread safe
    class GlobalLatestManager {
    public:
        GlobalLatestManager() = default;

        ~GlobalLatestManager() = default;

        void add_latest(const Row& row) {
            uint16_t vin_num = decode_vin(row.vin);
            std::lock_guard<std::mutex> l(_latest_mutexes[vin_num]);
            if (row.timestamp > _latest_records[vin_num].timestamp) {
                _latest_records[vin_num] = row;
            }
        }

        bool get_latest(uint16_t vin_num, const Vin& vin, const std::set<std::string>& requested_columns, Row &result_row) {
            Row latest_row;
            {
                std::lock_guard<std::mutex> l(_latest_mutexes[vin_num]);
                latest_row = _latest_records[vin_num];
            }
            if (latest_row.timestamp == 0) {
                return false;
            }
            result_row.vin = vin;
            result_row.timestamp = latest_row.timestamp;
            for (const auto& requested_column : requested_columns) {
                result_row.columns.emplace(requested_column, latest_row.columns.at(requested_column));
            }
            return true;
        }

        void save_latest_records_to_file(const Path& latest_records_path, SchemaSPtr schema) {
            std::ofstream output_file(latest_records_path, std::ios::out | std::ios::binary);
            if (!output_file.is_open()) {
                throw std::runtime_error("Failed to open file for writing.");
            }

            for (const auto & latest_record: _latest_records) {
                io::write_row_to_file(output_file, schema, latest_record, true);
            }

            output_file.flush();
            output_file.close();
        }

        void load_latest_records_from_file(const Path& latest_records_path, SchemaSPtr schema) {
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
                io::read_row_from_file(input_file, schema, true, row);
                uint16_t vin_num = decode_vin(row.vin);
                _latest_records[vin_num] = row;
            }

            input_file.close();
        }

    private:
        Row _latest_records[VIN_NUM_RANGE];
        std::mutex _latest_mutexes[VIN_NUM_RANGE];
    };
}