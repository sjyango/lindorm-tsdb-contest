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
            std::unique_lock<std::shared_mutex> l(_latest_mutexes[vin_num]);
            if (row.timestamp > _latest_records[vin_num].timestamp) {
                _latest_records[vin_num] = row;
            }
        }

        Row get_latest(uint16_t vin_num, const Vin& vin, const std::set<std::string>& requested_columns) {
            Row latest_row;
            {
                std::shared_lock<std::shared_mutex> l(_latest_mutexes[vin_num]);
                latest_row = _latest_records[vin_num];
            }
            Row result_row;
            result_row.vin = vin;
            result_row.timestamp = latest_row.timestamp;
            for (const auto& requested_column : requested_columns) {
                result_row.columns.emplace(requested_column, latest_row.columns.at(requested_column));
            }
            return result_row;
        }

        void save_latest_records_to_file(const Path& latest_records_path, SchemaSPtr schema) {
            std::ofstream output_file(latest_records_path, std::ios::out | std::ios::binary);
            if (!output_file.is_open()) {
                throw std::runtime_error("Failed to open file for writing.");
            }

            for (const auto & latest_record: _latest_records) {
                append_row_to_file(output_file, schema, latest_record);
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
                Row row = read_row_from_stream(input_file, schema);
                uint16_t vin_num = decode_vin(row.vin);
                _latest_records[vin_num] = row;
            }

            input_file.close();
        }

        void append_row_to_file(std::ofstream &fout, SchemaSPtr schema, const Row &row) {
            fout.write((const char *) row.vin.vin, VIN_LENGTH);
            fout.write((const char *) &row.timestamp, sizeof(int64_t));

            for (const auto &[column_name, column_type] : schema->columnTypeMap) {
                const ColumnValue &column_value = row.columns.at(column_name);
                int32_t rawSize = column_value.getRawDataSize();
                fout.write(column_value.columnData, rawSize);
            }
        }

        Row read_row_from_stream(std::ifstream &fin, SchemaSPtr schema) {
            Row row;
            fin.read((char *) row.vin.vin, VIN_LENGTH);
            fin.read((char *) &row.timestamp, sizeof(int64_t));

            for (const auto &[column_name, column_type] : schema->columnTypeMap) {
                switch (column_type) {
                    case COLUMN_TYPE_INTEGER: {
                        int32_t int_value;
                        fin.read((char *) &int_value, sizeof(int32_t));
                        ColumnValue column_value(int_value);
                        row.columns.emplace(column_name, std::move(column_value));
                        break;
                    }
                    case COLUMN_TYPE_DOUBLE_FLOAT: {
                        double_t double_value;
                        fin.read((char *) &double_value, sizeof(double_t));
                        ColumnValue column_value(double_value);
                        row.columns.emplace(column_name, std::move(column_value));
                        break;
                    }
                    case COLUMN_TYPE_STRING: {
                        int32_t str_length;
                        fin.read((char *) &str_length, sizeof(int32_t));
                        char *str_buf = new char[str_length];
                        fin.read(str_buf, str_length);
                        ColumnValue column_value(str_buf, str_length);
                        row.columns.emplace(column_name, std::move(column_value));
                        delete[]str_buf;
                        break;
                    }
                    default: {
                        throw std::runtime_error("Undefined column type, this is not expected");
                    }
                }
            }

            return row;
        }

    private:
        Row _latest_records[VIN_NUM_RANGE];
        std::shared_mutex _latest_mutexes[VIN_NUM_RANGE];
    };
}