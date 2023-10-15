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

#include <fstream>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include "base.h"
#include "struct/Row.h"

namespace LindormContest::io {

    static void stream_write_string_to_file(const Path &file_path, const std::string &buf) {
        std::ofstream output_file(file_path, std::ios::out | std::ios::binary);
        if (!output_file.is_open() || !output_file.good()) {
            throw std::runtime_error("open file failed");
        }
        output_file.write(buf.data(), buf.size());
        output_file.close();
    }

    static void stream_read_string_from_file(const Path &file_path, std::string &buf) {
        std::ifstream input_file(file_path, std::ios::in | std::ios::binary);
        if (!input_file.is_open() || !input_file.good()) {
            throw std::runtime_error("open file failed");
        }
        input_file.seekg(0, std::ios::end);
        auto file_size = input_file.tellg();
        input_file.seekg(0, std::ios::beg);
        buf.resize(file_size);
        input_file.read(buf.data(), file_size);
        input_file.close();
    }

    static void stream_read_string_from_file(const Path &file_path, uint32_t offset, uint32_t size, std::string &buf) {
        std::ifstream input_file(file_path, std::ios::in | std::ios::binary);
        if (!input_file.is_open() || !input_file.good()) {
            throw std::runtime_error("open file failed");
        }

        input_file.seekg(offset, std::ios::beg);
        buf.resize(size);
        input_file.read(buf.data(), size);
        input_file.close();
    }

    static uint32_t serialize_row(const Row &row, char *buf) {
        uint32_t row_length = 0;
        *reinterpret_cast<int64_t *>(buf) = row.timestamp;
        row_length += sizeof(int64_t);

        for (const auto &item: row.columns) {
            uint32_t raw_size = item.second.getRawDataSize();
            std::memcpy(buf + row_length, item.second.columnData, raw_size);
            row_length += raw_size;
        }

        return row_length;
    }

    static void serialize_row(const Row &row, bool vin_include, std::string &buf) {
        if (vin_include) {
            buf.append(row.vin.vin, VIN_LENGTH);
        }
        uint16_t ts_num = decode_ts(row.timestamp);
        buf.append((const char *) &ts_num, sizeof(uint16_t));

        for (const auto &item: row.columns) {
            buf.append(item.second.columnData, item.second.getRawDataSize());
        }
    }

    static void deserialize_row(SchemaSPtr schema, const char *&p, bool vin_include, Row &row) {
        if (vin_include) {
            std::memcpy(row.vin.vin, p, VIN_LENGTH);
            p += VIN_LENGTH;
        }

        row.timestamp = encode_ts(*reinterpret_cast<const uint16_t *>(p));
        p += sizeof(uint16_t);

        for (const auto &[column_name, column_type]: schema->columnTypeMap) {
            switch (column_type) {
                case COLUMN_TYPE_INTEGER: {
                    int32_t int_value = *reinterpret_cast<const int32_t *>(p);
                    p += sizeof(int32_t);
                    ColumnValue column_value(int_value);
                    row.columns.emplace(column_name, std::move(column_value));
                    break;
                }
                case COLUMN_TYPE_DOUBLE_FLOAT: {
                    double_t double_value = *reinterpret_cast<const double_t *>(p);
                    p += sizeof(double_t);
                    ColumnValue column_value(double_value);
                    row.columns.emplace(column_name, std::move(column_value));
                    break;
                }
                case COLUMN_TYPE_STRING: {
                    int32_t str_length = *reinterpret_cast<const int32_t *>(p);
                    p += sizeof(int32_t);
                    ColumnValue column_value(p, str_length);
                    p += str_length;
                    row.columns.emplace(column_name, std::move(column_value));
                    break;
                }
                default: {
                    throw std::runtime_error("Undefined column type, this is not expected");
                }
            }
        }
    }
}