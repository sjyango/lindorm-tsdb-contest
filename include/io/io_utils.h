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

    static void stream_write_string_to_file(const Path& file_path, const std::string& buf) {
        std::ofstream output_file(file_path, std::ios::out | std::ios::binary);
        if (!output_file.is_open() || !output_file.good()) {
            throw std::runtime_error("open file failed");
        }
        output_file << buf;
        output_file.flush();
        output_file.close();
    }

    static void stream_read_string_from_file(const Path& file_path, std::string& buf) {
        std::ifstream input_file(file_path, std::ios::in | std::ios::binary);
        if (!input_file.is_open() || !input_file.good()) {
            throw std::runtime_error("open file failed");
        }
        input_file >> buf;
        input_file.close();
    }

    static void stream_read_string_from_file(const Path& file_path, uint32_t offset, uint32_t size, std::string& buf) {
        std::ifstream input_file(file_path, std::ios::in | std::ios::binary);
        if (!input_file.is_open() || !input_file.good()) {
            throw std::runtime_error("open file failed");
        }

        input_file.seekg(offset, std::ios::beg);
        buf.resize(size);
        input_file.read(buf.data(), size);
        input_file.close();
    }

    static void mmap_read_string_from_file(const Path& file_path, std::string& buf) {
        int fd = open(file_path.c_str(), O_RDONLY);
        if (fd == -1) {
            throw std::runtime_error("open file failed");
        }
        off_t file_size = lseek(fd, 0, SEEK_END);
        void* file_memory = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (file_memory == MAP_FAILED) {
            throw std::runtime_error("unable to map file to memory");
        }
        const char* file_content = static_cast<const char*>(file_memory);
        buf.assign(file_content, file_size);
        munmap(file_memory, file_size);
        close(fd);
    }

    static void write_row_to_file(std::ofstream &out, SchemaSPtr schema, const Row &row, bool vin_include) {
        if (row.columns.size() != SCHEMA_COLUMN_NUMS) {
            std::cerr << "Cannot write a non-complete row with columns' num: [" << row.columns.size() << "]. ";
            std::cerr << "There is [" << SCHEMA_COLUMN_NUMS << "] rows in total" << std::endl;
            throw std::exception();
        }

        if (vin_include) {
            out.write((const char *) row.vin.vin, VIN_LENGTH);
        }
        out.write((const char *) &row.timestamp, sizeof(int64_t));

        for (const auto &[column_name, column_type] : schema->columnTypeMap) {
            const ColumnValue &column_value = row.columns.at(column_name);
            int32_t raw_size = column_value.getRawDataSize();
            out.write(column_value.columnData, raw_size);
        }

        out.flush();
    }

    static bool read_row_from_file(std::ifstream &in, SchemaSPtr schema, bool vin_include, Row& row) {
        if (in.fail() || in.eof()) {
            return false;
        }
        if (vin_include) {
            in.read((char *) row.vin.vin, VIN_LENGTH);
        }
        in.read((char *) &row.timestamp, sizeof(int64_t));
        if (in.fail() || in.eof()) {
            return false;
        }

        for (const auto &[column_name, column_type] : schema->columnTypeMap) {
            switch (column_type) {
                case COLUMN_TYPE_INTEGER: {
                    int32_t int_value;
                    in.read((char *) &int_value, sizeof(int32_t));
                    ColumnValue column_value(int_value);
                    row.columns.emplace(column_name, std::move(column_value));
                    break;
                }
                case COLUMN_TYPE_DOUBLE_FLOAT: {
                    double_t double_value;
                    in.read((char *) &double_value, sizeof(double_t));
                    ColumnValue column_value(double_value);
                    row.columns.emplace(column_name, std::move(column_value));
                    break;
                }
                case COLUMN_TYPE_STRING: {
                    int32_t str_length;
                    in.read((char *) &str_length, sizeof(int32_t));
                    char *str_buf = new char[str_length];
                    in.read(str_buf, str_length);
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

        return true;
    }
}




// static void mmap_write_string_to_file(const Path& file_path, const std::string& buf) {
//     int fd = open(file_path.c_str(), O_RDWR | O_CREAT);
//     if (fd == -1) {
//         throw std::runtime_error("open file failed");
//     }
//     size_t buf_size = buf.size();
//     if (ftruncate(fd, buf_size) == -1) {
//         throw std::runtime_error("set file size failed");
//     }
//     void* file_memory = mmap(nullptr, buf_size, PROT_WRITE, MAP_SHARED, fd, 0);
//     if (file_memory == MAP_FAILED) {
//         throw std::runtime_error("unable to map file to memory");
//     }
//     std::memcpy(file_memory, buf.c_str(), buf_size);
//     munmap(file_memory, buf_size);
//     close(fd);
// }


// static void mmap_read_string_from_file(const Path& file_path, uint32_t offset, uint32_t size, std::string& buf) {
//     int fd = open(file_path.c_str(), O_RDONLY);
//     if (fd == -1) {
//         throw std::runtime_error("open file failed");
//     }
//     off_t file_size = lseek(fd, 0, SEEK_END);
//     if (offset >= file_size || size == 0 || offset + size > file_size) {
//         throw std::runtime_error("unable to map file to memory");
//     }
//     void* file_memory = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, offset);
//     if (file_memory == MAP_FAILED) {
//         perror("mmap error");
//         throw std::runtime_error("unable to map file to memory");
//     }
//     const char* file_content = static_cast<const char*>(file_memory);
//     buf.assign(file_content, size);
//     munmap(file_memory, size);
//     close(fd);
// }