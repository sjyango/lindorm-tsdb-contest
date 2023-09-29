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
#include "common/pdqsort.h"

namespace LindormContest {

    // multi thread safe
    class CompactionManager {
    public:
        CompactionManager() = default;

        CompactionManager(uint16_t vin_num, const Path& root_path)
                : _vin_num(vin_num), _compaction_nums(0), _root_path(root_path), _schema(nullptr) {
            Path vin_dir_path = _root_path / "compaction" / std::to_string(_vin_num);
            std::filesystem::create_directories(vin_dir_path);
            _latest_row.vin = encode_vin(_vin_num);
        }

        CompactionManager(CompactionManager &&other) = default;

        ~CompactionManager() = default;

        void set_schema(SchemaSPtr schema) {
            _schema = schema;
        }

        void level_compaction(uint16_t start_file, uint16_t end_file) {
            std::vector<Row> input_rows;
            std::vector<uint32_t> input_indexes;

            for (uint16_t i = start_file; i < end_file; ++i) {
                Path flush_file_path = _root_path / "no-compaction" / std::to_string(_vin_num) / std::to_string(i);
                assert(std::filesystem::exists(flush_file_path));
                std::ifstream input_file;
                input_file.open(flush_file_path, std::ios::in | std::ios::binary);
                assert(input_file.is_open() && input_file.good());

                for (uint16_t j = 0; j < FILE_FLUSH_SIZE && !input_file.eof(); ++j) {
                    Row row;
                    io::read_row_from_file(input_file, _schema, false, row);
                    input_rows.emplace_back(std::move(row));
                }

                input_file.close();
            }

            input_indexes.resize(input_rows.size());
            std::iota(input_indexes.begin(),input_indexes.end(),0);

            pdqsort_branchless(input_indexes.begin(), input_indexes.end(), [&input_rows] (uint32_t lhs, uint32_t rhs) {
                return input_rows[lhs].timestamp < input_rows[rhs].timestamp;
            });

            int64_t last_ts = input_rows[input_indexes.back()].timestamp;

            if (last_ts > _latest_row.timestamp) {
                _latest_row.timestamp = last_ts;
                _latest_row.columns = input_rows[input_indexes.back()].columns;
            }

            TsmFile output_tsm_file;
            _multiway_compaction(_schema, input_rows, input_indexes, output_tsm_file);
            Path output_tsm_file_path = _root_path / "compaction" / std::to_string(_vin_num) / std::to_string(_compaction_nums++);
            output_tsm_file.write_to_file(output_tsm_file_path);
        }

        Row get_latest_row() const {
            return _latest_row;
        }

    private:
        // input_rows are sorted
        static void _multiway_compaction(SchemaSPtr schema, const std::vector<Row> &input_rows,
                                         const std::vector<uint32_t>& row_indexes, TsmFile &output_file) {
            size_t row_nums = input_rows.size();
            std::vector<int64_t> tss(row_nums);

            for (size_t i = 0; i < row_nums; ++i) {
                tss[i] = input_rows[row_indexes[i]].timestamp;
            }

            for (const auto &[column_name, column_type]: schema->columnTypeMap) {
                std::vector<ColumnValue> column_value(row_nums);

                for (size_t i = 0; i < row_nums; ++i) {
                    column_value[i] = input_rows[row_indexes[i]].columns.at(column_name);
                }

                IndexBlock index_block(column_name, column_type);

                for (size_t start = 0; start < row_nums; start += DATA_BLOCK_ITEM_NUMS) {
                    size_t end = std::min(start + DATA_BLOCK_ITEM_NUMS, row_nums);
                    DataBlock data_block(column_value.begin() + start, column_value.begin() + end);
                    IndexEntry index_entry;
                    index_entry._min_time_index = start;
                    index_entry._max_time_index = end - 1;
                    index_entry._min_time = tss[index_entry._min_time_index];
                    index_entry._max_time = tss[index_entry._max_time_index];
                    index_entry._count = end - start;
                    switch (column_type) {
                        case COLUMN_TYPE_INTEGER: {
                            int64_t int_sum = 0;
                            int32_t int_max = std::numeric_limits<int32_t>::lowest();
                            for (const auto &v: data_block._column_values) {
                                int32_t int_value;
                                v.getIntegerValue(int_value);
                                int_sum += int_value;
                                int_max = std::max(int_max, int_value);
                            }
                            index_entry.set_sum(int_sum);
                            index_entry.set_max(int_max);
                            break;
                        }
                        case COLUMN_TYPE_DOUBLE_FLOAT: {
                            double_t double_sum = 0.0;
                            double_t double_max = std::numeric_limits<double_t>::lowest();
                            for (const auto &v: data_block._column_values) {
                                double_t double_value;
                                v.getDoubleFloatValue(double_value);
                                double_sum += double_value;
                                double_max = std::max(double_max, double_value);
                            }
                            index_entry.set_sum(double_sum);
                            index_entry.set_max(double_max);
                            break;
                        }
                        case COLUMN_TYPE_STRING: {
                            break;
                        }
                        default: {
                            throw std::runtime_error("invalid variant type");
                        }
                    }
                    output_file._data_blocks.emplace_back(std::move(data_block));
                    index_block.add_entry(index_entry); // one index entry corresponds one data block
                }

                output_file._index_blocks.emplace_back(std::move(index_block)); // one column data corresponds one index block
            }

            output_file._footer._tss = std::move(tss);
        }

        uint16_t _vin_num;
        uint16_t _compaction_nums;
        Path _root_path;
        SchemaSPtr _schema;
        Row _latest_row;
    };

    class GlobalCompactionManager;

    using GlobalCompactionManagerSPtr = std::shared_ptr<GlobalCompactionManager>;

    class GlobalCompactionManager {
    public:
        GlobalCompactionManager(const Path &root_path) {
            _thread_pool = std::make_unique<ThreadPool>(POOL_THREAD_NUM);
            for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
                _compaction_managers[vin_num] = std::make_unique<CompactionManager>(vin_num, root_path);
            }
        }

        void set_schema(SchemaSPtr schema) {
            for (auto &compaction_manager: _compaction_managers) {
                compaction_manager->set_schema(schema);
            }
        }

        void level_compaction_async(uint16_t vin_num, uint16_t start_file, uint16_t end_file) {
            _thread_pool->submit(do_level_compaction, _compaction_managers[vin_num].get(), start_file, end_file);
        }

        static void do_level_compaction(CompactionManager *compaction_manager, uint16_t start_file, uint16_t end_file) {
            compaction_manager->level_compaction(start_file, end_file);
        }

        void finalize_compaction() {
            _thread_pool->shutdown();
            assert(_thread_pool->empty());
        }

        void save_latest_records_to_file(const Path& latest_records_path, SchemaSPtr schema) const {
            std::ofstream output_file(latest_records_path, std::ios::out | std::ios::binary);
            if (!output_file.is_open()) {
                throw std::runtime_error("Failed to open file for writing.");
            }

            for (uint16_t i = 0; i < VIN_NUM_RANGE; ++i) {
                io::write_row_to_file(output_file, schema, _compaction_managers[i]->get_latest_row(), true);
            }

            output_file.flush();
            output_file.close();
        }

    private:
        ThreadPoolUPtr _thread_pool;
        std::unique_ptr<CompactionManager> _compaction_managers[VIN_NUM_RANGE];
    };

}
