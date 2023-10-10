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

#include <thread>
#include <atomic>
#include <sstream>
#include <numeric>

#include "base.h"
#include "struct/Vin.h"
#include "struct/Schema.h"
#include "index_manager.h"

namespace LindormContest {

    // multi thread safe
    class ConvertManager {
    public:
        ConvertManager() = default;

        ConvertManager(uint16_t vin_num, const Path& root_path) : _vin_num(vin_num), _schema(nullptr) {
            _no_compaction_path = root_path / "no-compaction" / std::to_string(_vin_num);
            _compaction_path = root_path / "compaction" / std::to_string(_vin_num);
            std::filesystem::create_directories(_compaction_path);
            _latest_row.vin = encode_vin(_vin_num);
            _latest_row.timestamp = MAX_TS;
        }

        ConvertManager(ConvertManager &&other) = default;

        ~ConvertManager() = default;

        void init(SchemaSPtr schema) {
            _schema = schema;
        }

        void convert(uint16_t file_idx) {
            TsmFile output_tsm_file;
            Path flush_file_path = _no_compaction_path / std::to_string(file_idx);
            std::string buf;
            io::stream_read_string_from_file(flush_file_path, buf);
            std::array<Row, FILE_CONVERT_SIZE> sorted_rows;
            const char* start = buf.c_str();
            const char* end = start + buf.size();

            while (start < end) {
                Row row;
                io::deserialize_row(_schema, start, false, row);
                uint16_t ts_num = decode_ts(row.timestamp) % FILE_CONVERT_SIZE;
                swap_row(sorted_rows[ts_num], row);
            }

            for (const auto& [column_name, column_type] : _schema->columnTypeMap) {
                switch (column_type) {
                    case COLUMN_TYPE_INTEGER:
                        convert_int_column(file_idx, sorted_rows, column_name, output_tsm_file._data_blocks, output_tsm_file._index_blocks);
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        convert_double_column(file_idx, sorted_rows, column_name, output_tsm_file._data_blocks, output_tsm_file._index_blocks);
                        break;
                    case COLUMN_TYPE_STRING:
                        convert_string_column(file_idx, sorted_rows, column_name, output_tsm_file._data_blocks, output_tsm_file._index_blocks);
                        break;
                    default:
                        break;
                }
            }

            Path output_tsm_file_path = _compaction_path / std::to_string(file_idx);
            output_tsm_file.write_to_file(output_tsm_file_path);
        }

        void convert_int_column(uint16_t file_idx, std::array<Row, FILE_CONVERT_SIZE>& rows, const std::string& column_name,
                                std::vector<std::unique_ptr<DataBlock>>& data_blocks, std::vector<IndexBlock>& index_blocks) {
            int32_t src_values[FILE_CONVERT_SIZE];

            for (uint16_t i = 0; i < FILE_CONVERT_SIZE; ++i) {
                rows[i].columns[column_name].getIntegerValue(src_values[i]);
            }

            if (unlikely(file_idx == TSM_FILE_COUNT - 1)) {
                _latest_row.columns[column_name] = ColumnValue(src_values[FILE_CONVERT_SIZE - 1]);
            }

            IndexBlock index_block(column_name, COLUMN_TYPE_INTEGER);

            for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                std::unique_ptr<IntDataBlock> data_block = std::make_unique<IntDataBlock>();
                std::memcpy(data_block->_column_values, src_values + i * DATA_BLOCK_ITEM_NUMS, DATA_BLOCK_ITEM_NUMS * sizeof(int32_t));
                IndexEntry index_entry;
                int64_t int_sum = 0;
                int32_t int_max = std::numeric_limits<int32_t>::lowest();
                int32_t int_min = std::numeric_limits<int32_t>::max();

                for (auto v: data_block->_column_values) {
                    int_sum += v;
                    int_max = std::max(int_max, v);
                    int_min = std::min(int_min, v);
                }

                index_entry.set_sum(int_sum);
                index_entry.set_max(int_max);
                data_blocks.emplace_back(std::move(data_block));
                index_block.add_entry(index_entry);
            }

            index_blocks.emplace_back(std::move(index_block));
        }

        void convert_double_column(uint16_t file_idx, std::array<Row, FILE_CONVERT_SIZE>& rows, const std::string& column_name,
                                std::vector<std::unique_ptr<DataBlock>>& data_blocks, std::vector<IndexBlock>& index_blocks) {
            double_t src_values[FILE_CONVERT_SIZE];

            for (uint16_t i = 0; i < FILE_CONVERT_SIZE; ++i) {
                rows[i].columns[column_name].getDoubleFloatValue(src_values[i]);
            }

            if (unlikely(file_idx == TSM_FILE_COUNT - 1)) {
                _latest_row.columns[column_name] = ColumnValue(src_values[FILE_CONVERT_SIZE - 1]);
            }

            IndexBlock index_block(column_name, COLUMN_TYPE_DOUBLE_FLOAT);

            for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                std::unique_ptr<DoubleDataBlock> data_block = std::make_unique<DoubleDataBlock>();
                std::memcpy(data_block->_column_values, src_values + i * DATA_BLOCK_ITEM_NUMS, DATA_BLOCK_ITEM_NUMS * sizeof(double_t));
                IndexEntry index_entry;
                double_t double_sum = 0;
                double_t double_max = std::numeric_limits<double_t>::lowest();

                for (auto v: data_block->_column_values) {
                    double_sum += v;
                    double_max = std::max(double_max, v);
                }

                index_entry.set_sum(double_sum);
                index_entry.set_max(double_max);
                data_blocks.emplace_back(std::move(data_block));
                index_block.add_entry(index_entry);
            }

            index_blocks.emplace_back(std::move(index_block));
        }

        void convert_string_column(uint16_t file_idx, std::array<Row, FILE_CONVERT_SIZE>& rows, const std::string& column_name,
                                  std::vector<std::unique_ptr<DataBlock>>& data_blocks, std::vector<IndexBlock>& index_blocks) {
            ColumnValue src_values[FILE_CONVERT_SIZE];

            for (uint16_t i = 0; i < FILE_CONVERT_SIZE; ++i) {
                src_values[i] = rows[i].columns[column_name];
            }

            if (unlikely(file_idx == TSM_FILE_COUNT - 1)) {
                _latest_row.columns[column_name] = src_values[FILE_CONVERT_SIZE - 1];
            }

            IndexBlock index_block(column_name, COLUMN_TYPE_STRING);

            for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                std::unique_ptr<StringDataBlock> data_block = std::make_unique<StringDataBlock>();

                for (uint16_t j = 0; j < DATA_BLOCK_ITEM_NUMS; ++j) {
                    std::swap(data_block->_column_values[j], src_values[i * DATA_BLOCK_ITEM_NUMS + j]);
                }

                IndexEntry index_entry;
                data_blocks.emplace_back(std::move(data_block));
                index_block.add_entry(index_entry);
            }

            index_blocks.emplace_back(std::move(index_block));
        }

        Row get_latest_row() const {
            return _latest_row;
        }

    private:
        uint16_t _vin_num;
        Path _no_compaction_path;
        Path _compaction_path;
        SchemaSPtr _schema;
        Row _latest_row;
    };

    class GlobalConvertManager;

    using GlobalConvertManagerSPtr = std::shared_ptr<GlobalConvertManager>;

    class GlobalConvertManager {
    public:
        GlobalConvertManager(const Path &root_path) {
            _thread_pool = std::make_unique<ThreadPool>(POOL_THREAD_NUM);
            for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
                _convert_managers[vin_num] = std::make_unique<ConvertManager>(vin_num, root_path);
            }
        }

        void init(SchemaSPtr schema) {
            for (auto &convert_manager: _convert_managers) {
                convert_manager->init(schema);
            }
        }

        void convert_async(uint16_t vin_num, uint16_t file_idx) {
            _thread_pool->submit(do_convert, _convert_managers[vin_num].get(), file_idx);
        }

        static void do_convert(ConvertManager *convert_manager, uint16_t file_idx) {
            convert_manager->convert(file_idx);
        }

        void finalize_convert() {
            _thread_pool->shutdown();
            assert(_thread_pool->empty());
        }

        void save_latest_records_to_file(const Path& latest_records_path, SchemaSPtr schema) const {
            std::ofstream output_file(latest_records_path, std::ios::out | std::ios::binary);
            if (!output_file.is_open()) {
                throw std::runtime_error("Failed to open file for writing.");
            }

            for (uint16_t i = 0; i < VIN_NUM_RANGE; ++i) {
                io::write_row_to_file(output_file, schema, _convert_managers[i]->get_latest_row(), true);
            }

            output_file.flush();
            output_file.close();
        }

    private:
        ThreadPoolUPtr _thread_pool;
        std::unique_ptr<ConvertManager> _convert_managers[VIN_NUM_RANGE];
    };

}
