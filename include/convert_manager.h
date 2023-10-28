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
            std::map<std::string, std::vector<std::unique_ptr<DataBlock>>> sorted_columns;

            for (const auto &[column_name, column_type]: _schema->columnTypeMap) {
                switch (column_type) {
                    case COLUMN_TYPE_INTEGER: {
                        std::vector<std::unique_ptr<DataBlock>> data_blocks;
                        for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                            data_blocks.emplace_back(std::make_unique<IntDataBlock>());
                        }
                        sorted_columns.emplace(column_name, std::move(data_blocks));
                        break;
                    }
                    case COLUMN_TYPE_DOUBLE_FLOAT: {
                        std::vector<std::unique_ptr<DataBlock>> data_blocks;
                        for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                            data_blocks.emplace_back(std::make_unique<DoubleDataBlock>());
                        }
                        sorted_columns.emplace(column_name, std::move(data_blocks));
                        break;
                    }
                    case COLUMN_TYPE_STRING: {
                        std::vector<std::unique_ptr<DataBlock>> data_blocks;
                        for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                            data_blocks.emplace_back(std::make_unique<StringDataBlock>());
                        }
                        sorted_columns.emplace(column_name, std::move(data_blocks));
                        break;
                    }
                    default:
                        break;
                }
            }

            const char* start = buf.c_str();
            const char* end = start + buf.size();

            while (start < end) {
                deserialize_row(start, sorted_columns);
            }

            assert(start == end);

            for (auto &[column_name, data_blocks]: sorted_columns) {
                IndexBlock index_block;

                switch (_schema->columnTypeMap[column_name]) {
                    case COLUMN_TYPE_INTEGER: {
                        for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                            IntDataBlock& int_data_block = dynamic_cast<IntDataBlock&>(*data_blocks[i]);
                            uint32_t range_width = int_data_block._max - int_data_block._min + 1;

                            if (range_width == 1) {
                                int_data_block._type = IntCompressType::SAME;
                            } else if (range_width <= BITPACKING_RANGE_NUM) {
                                int_data_block._type = IntCompressType::BITPACK;
                                int_data_block._required_bits = get_next_power_of_two(range_width);
                            } else {
                                int_data_block._type = IntCompressType::FASTPFOR;
                            }

                            index_block._index_entries[i].set_sum(int_data_block._sum);
                            index_block._index_entries[i].set_max(int_data_block._max);
                            output_tsm_file._data_blocks.emplace_back(std::move(data_blocks[i]));
                        }
                        break;
                    }
                    case COLUMN_TYPE_DOUBLE_FLOAT: {
                        for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                            DoubleDataBlock& double_data_block = dynamic_cast<DoubleDataBlock&>(*data_blocks[i]);

                            if (double_data_block._max == double_data_block._min) {
                                double_data_block._type = DoubleCompressType::SAME;
                            } else {
                                double_data_block._type = DoubleCompressType::CHIMP;
                            }

                            index_block._index_entries[i].set_sum(double_data_block._sum);
                            index_block._index_entries[i].set_max(double_data_block._max);
                            output_tsm_file._data_blocks.emplace_back(std::move(data_blocks[i]));
                        }
                        break;
                    }
                    case COLUMN_TYPE_STRING: {
                        for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                            output_tsm_file._data_blocks.emplace_back(std::move(data_blocks[i]));
                        }
                        break;
                    }
                    default:
                        break;
                }

                output_tsm_file._index_blocks.emplace_back(std::move(index_block));
            }

            Path output_tsm_file_path = _compaction_path / std::to_string(file_idx);
            output_tsm_file.write_to_file(output_tsm_file_path);
        }

        inline void deserialize_row(const char*& p, std::map<std::string, std::vector<std::unique_ptr<DataBlock>>>& sorted_columns) {
            uint16_t ts_num = decode_ts(*reinterpret_cast<const int64_t *>(p)) % FILE_CONVERT_SIZE;
            p += sizeof(int64_t);
            uint16_t block_index = ts_num / DATA_BLOCK_ITEM_NUMS;
            uint16_t block_offset = ts_num % DATA_BLOCK_ITEM_NUMS;

            for (auto &[column_name, column_blocks]: sorted_columns) {
                switch (_schema->columnTypeMap[column_name]) {
                    case COLUMN_TYPE_INTEGER: {
                        IntDataBlock& int_data_block = dynamic_cast<IntDataBlock&>(*column_blocks[block_index]);
                        int32_t int_value = *reinterpret_cast<const int32_t *>(p);
                        p += sizeof(int32_t);
                        int_data_block._column_values[block_offset] = int_value;
                        int_data_block._sum += int_value;
                        int_data_block._min = std::min(int_data_block._min, int_value);
                        int_data_block._max = std::max(int_data_block._max, int_value);
                        break;
                    }
                    case COLUMN_TYPE_DOUBLE_FLOAT: {
                        DoubleDataBlock& double_data_block = dynamic_cast<DoubleDataBlock&>(*column_blocks[block_index]);
                        double_t double_value = *reinterpret_cast<const double_t *>(p);
                        p += sizeof(double_t);
                        double_data_block._column_values[block_offset] = double_value;
                        double_data_block._sum += double_value;
                        double_data_block._min = std::min(double_data_block._min, double_value);
                        double_data_block._max = std::max(double_data_block._max, double_value);
                        break;
                    }
                    case COLUMN_TYPE_STRING: {
                        StringDataBlock& string_data_block = dynamic_cast<StringDataBlock&>(*column_blocks[block_index]);
                        int32_t str_length = *reinterpret_cast<const int32_t *>(p);
                        p += sizeof(int32_t);
                        ColumnValue column_value(p, str_length);
                        p += str_length;
                        string_data_block._column_values[block_offset] = column_value;
                        if (unlikely(string_data_block._str_length == std::numeric_limits<uint8_t>::max())) {
                            string_data_block._str_length = str_length;
                        }
                        if (string_data_block._same_length) {
                            string_data_block._same_length = string_data_block._str_length == str_length;
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
        }


        // void convert_double_column(uint16_t file_idx, std::array<Row, FILE_CONVERT_SIZE>& rows, const std::string& column_name,
        //                         std::vector<std::unique_ptr<DataBlock>>& data_blocks, std::vector<IndexBlock>& index_blocks) {
        //     double_t src_values[FILE_CONVERT_SIZE];
        //
        //     for (uint16_t i = 0; i < FILE_CONVERT_SIZE; ++i) {
        //         rows[i].columns[column_name].getDoubleFloatValue(src_values[i]);
        //     }
        //
        //     if (unlikely(file_idx == TSM_FILE_COUNT - 1)) {
        //         _latest_row.columns[column_name] = ColumnValue(src_values[FILE_CONVERT_SIZE - 1]);
        //     }
        //
        //     IndexBlock index_block;
        //
        //     for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
        //         std::unique_ptr<DoubleDataBlock> data_block = std::make_unique<DoubleDataBlock>();
        //         std::memcpy(data_block->_column_values.data(), src_values + i * DATA_BLOCK_ITEM_NUMS, DATA_BLOCK_ITEM_NUMS * sizeof(double_t));
        //         IndexEntry index_entry;
        //         double_t double_sum = 0;
        //         double_t double_max = std::numeric_limits<double_t>::lowest();
        //         double_t double_min = std::numeric_limits<double_t>::max();
        //
        //         for (auto v: data_block->_column_values) {
        //             double_sum += v;
        //             double_max = std::max(double_max, v);
        //             double_min = std::min(double_min, v);
        //         }
        //
        //         if (double_max == double_min) {
        //             data_block->_type = DoubleCompressType::SAME;
        //         } else {
        //             data_block->_type = DoubleCompressType::GORILLA;
        //         }
        //
        //         index_entry.set_sum(double_sum);
        //         index_entry.set_max(double_max);
        //         data_blocks.emplace_back(std::move(data_block));
        //         index_block.add_entry(index_entry);
        //     }
        //
        //     index_blocks.emplace_back(std::move(index_block));
        // }
        //
        // void convert_string_column(uint16_t file_idx, std::array<Row, FILE_CONVERT_SIZE>& rows, const std::string& column_name,
        //                           std::vector<std::unique_ptr<DataBlock>>& data_blocks, std::vector<IndexBlock>& index_blocks) {
        //     ColumnValue src_values[FILE_CONVERT_SIZE];
        //
        //     for (uint16_t i = 0; i < FILE_CONVERT_SIZE; ++i) {
        //         src_values[i] = rows[i].columns[column_name];
        //     }
        //
        //     if (unlikely(file_idx == TSM_FILE_COUNT - 1)) {
        //         _latest_row.columns[column_name] = src_values[FILE_CONVERT_SIZE - 1];
        //     }
        //
        //     IndexBlock index_block;
        //
        //     for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
        //         std::unique_ptr<StringDataBlock> data_block = std::make_unique<StringDataBlock>();
        //
        //         for (uint16_t j = 0; j < DATA_BLOCK_ITEM_NUMS; ++j) {
        //             std::swap(data_block->_column_values[j], src_values[i * DATA_BLOCK_ITEM_NUMS + j]);
        //         }
        //
        //         IndexEntry index_entry;
        //         data_blocks.emplace_back(std::move(data_block));
        //         index_block.add_entry(index_entry);
        //     }
        //
        //     index_blocks.emplace_back(std::move(index_block));
        // }

    private:
        uint16_t _vin_num;
        Path _no_compaction_path;
        Path _compaction_path;
        SchemaSPtr _schema;
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

        // void save_latest_records_to_file(const Path& latest_records_path) const {
        //     std::ofstream output_file(latest_records_path, std::ios::out | std::ios::binary);
        //     if (!output_file.is_open()) {
        //         throw std::runtime_error("Failed to open file for writing.");
        //     }
        //     std::string buf;
        //
        //     for (uint16_t i = 0; i < VIN_NUM_RANGE; ++i) {
        //         io::serialize_row(_convert_managers[i]->get_latest_row(), true, buf);
        //     }
        //
        //     output_file.write(buf.c_str(), buf.size());
        //     output_file.close();
        // }

    private:
        ThreadPoolUPtr _thread_pool;
        std::unique_ptr<ConvertManager> _convert_managers[VIN_NUM_RANGE];
    };

}
