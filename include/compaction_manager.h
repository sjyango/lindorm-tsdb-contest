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

#include "base.h"
#include "struct/Vin.h"
#include "struct/Schema.h"
#include "index_manager.h"

namespace LindormContest {

    class CompactionManager {
    public:
        CompactionManager() = default;

        CompactionManager(uint16_t vin_num, const Path& vin_dir_path, GlobalIndexManagerSPtr index_manager)
        : _vin_num(vin_num), _vin_dir_path(vin_dir_path), _schema(nullptr), _index_manager(index_manager) {}

        CompactionManager(CompactionManager&& other) = default;

        ~CompactionManager() = default;

        void set_schema(SchemaSPtr schema) {
            _schema = schema;
        }

        void level_compaction(int32_t level) {
            std::vector<std::string> tsm_file_names;
            std::vector<TsmFile> input_tsm_files;
            _get_file_names(level, tsm_file_names);
            _get_tsm_files(tsm_file_names, input_tsm_files);
            TsmFile output_tsm_file;
            _multiway_compaction(_schema, input_tsm_files, output_tsm_file);
            std::string output_tsm_file_name = std::to_string(_compaction_nums) + "-" + std::to_string(level + 1) + ".tsm";
            Path output_tsm_file_path = _vin_dir_path / output_tsm_file_name;
            _index_manager->insert_indexes(_vin_num, output_tsm_file_name, output_tsm_file._index_blocks);
            output_tsm_file.write_to_file(output_tsm_file_path);
            _delete_tsm_files(tsm_file_names);
            _compaction_nums++;
        }

    private:
        struct CompactionRecord {
            int64_t _ts;
            ColumnValue _value;

            CompactionRecord(int64_t ts, const ColumnValue& value) : _ts(ts), _value(value) {}
        };

        struct CompareFunc {
            bool operator()(const CompactionRecord& lhs, const CompactionRecord& rhs) {
                return lhs._ts > rhs._ts; // min heap
            }
        };

        void _get_file_names(int32_t level, std::vector<std::string>& tsm_file_names) {
            std::string suffix = std::to_string(level) + ".tsm";
            for (const auto& entry: std::filesystem::directory_iterator(_vin_dir_path)) {
                if (entry.is_regular_file() && _path_ends_with(entry.path().filename(), suffix)) {
                    tsm_file_names.emplace_back(entry.path().filename());
                }
            }
        }

        inline bool _path_ends_with(const std::string& path, const std::string& suffix) {
            if (path.length() < suffix.length()) {
                return false;
            }
            return path.compare(path.length() - suffix.length(), suffix.length(), suffix) == 0;
        }

        void _delete_tsm_files(const std::vector<std::string>& tsm_file_names) {
            for (const auto &tsm_file_name: tsm_file_names) {
                Path tsm_file_path = _vin_dir_path / tsm_file_name;
                if (std::filesystem::exists(tsm_file_path)) {
                    std::filesystem::remove(tsm_file_path);
                    _index_manager->remove_indexes(_vin_num, tsm_file_name);
                }
            }
        }

        void _get_tsm_files(const std::vector<std::string>& tsm_file_names, std::vector<TsmFile>& tsm_files) {
            for (const auto &tsm_file_name: tsm_file_names) {
                Path tsm_file_path = _vin_dir_path / tsm_file_name;
                TsmFile tsm_file;
                tsm_file.read_from_file(tsm_file_path);
                tsm_files.emplace_back(std::move(tsm_file));
            }
        }

        static void _multiway_compaction(SchemaSPtr schema, const std::vector<TsmFile>& input_files, TsmFile& output_file) {
            std::map<std::string, std::priority_queue<CompactionRecord, std::vector<CompactionRecord>, CompareFunc>> _min_heaps;

            for (const auto &input_file: input_files) {
                for (const auto &column: schema->columnTypeMap) {
                    if (unlikely(_min_heaps.find(column.first) == _min_heaps.cend())) {
                        std::priority_queue<CompactionRecord, std::vector<CompactionRecord>, CompareFunc> pq;
                        _min_heaps.emplace(column.first, std::move(pq));
                    }
                    auto ts_iter = input_file._footer._tss.cbegin();
                    auto ts_end_iter = input_file._footer._tss.cend();
                    auto column_iter = input_file.column_begin(column.first);
                    auto column_end_iter = input_file.column_end(column.first);

                    while (column_iter != column_end_iter) {
                        _min_heaps[column.first].emplace(*ts_iter, *column_iter);
                        ++column_iter;
                        ++ts_iter;
                    }

                    assert(ts_iter == ts_end_iter);
                }
            }

            bool has_collect_tss = false;
            std::vector<int64_t> tss;

            for (auto &[column_name, min_heap]: _min_heaps) {
                std::vector<ColumnValue> column_value;

                while (!min_heap.empty()) {
                    const auto& record = min_heap.top();
                    column_value.emplace_back(record._value);
                    if (unlikely(!has_collect_tss)) {
                        tss.emplace_back(record._ts);
                    }
                    min_heap.pop();
                }

                has_collect_tss = true;
                const size_t value_size = column_value.size();
                ColumnType type = schema->columnTypeMap[column_name];
                IndexBlock index_block(column_name, type);

                for (size_t start = 0; start < value_size; start += DATA_BLOCK_ITEM_NUMS) {
                    size_t end = std::min(start + DATA_BLOCK_ITEM_NUMS, value_size);
                    DataBlock data_block(column_value.begin() + start, column_value.begin() + end);
                    IndexEntry index_entry;
                    index_entry._min_time_index = start;
                    index_entry._max_time_index = end - 1;
                    index_entry._min_time = tss[index_entry._min_time_index];
                    index_entry._max_time = tss[index_entry._max_time_index];
                    index_entry._count = end - start;
                    switch (type) {
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
        Path _vin_dir_path;
        SchemaSPtr _schema;
        GlobalIndexManagerSPtr _index_manager;
        uint16_t _compaction_nums;
    };

    class GlobalCompactionManager;

    using GlobalCompactionManagerUPtr = std::unique_ptr<GlobalCompactionManager>;

    class GlobalCompactionManager {
    public:
        GlobalCompactionManager(const Path& root_path, GlobalIndexManagerSPtr index_manager) {
            _thread_pool = std::make_unique<ThreadPool>(std::thread::hardware_concurrency());
            for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
                Path vin_dir_path = root_path / std::to_string(vin_num);
                _compaction_managers[vin_num] = std::make_unique<CompactionManager>(vin_num, vin_dir_path, index_manager);
            }
        }

        void set_schema(SchemaSPtr schema) {
            for (auto &compaction_manager: _compaction_managers) {
                compaction_manager->set_schema(schema);
            }
        }

        static void level_compaction(CompactionManager* compaction_manager, int32_t level = 0) {
            compaction_manager->level_compaction(level);
        }

        void level_compaction_all(int32_t level = 0) {
            for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
                _thread_pool->submit(level_compaction, _compaction_managers[vin_num].get(), level);
            }
            _thread_pool->shutdown();
        }

    private:
        ThreadPoolUPtr _thread_pool;
        std::unique_ptr<CompactionManager> _compaction_managers[VIN_NUM_RANGE];
    };

}