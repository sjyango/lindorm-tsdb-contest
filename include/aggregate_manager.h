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
#include <variant>
#include <numeric>
#include <optional>
#include <limits>

#include "struct/Vin.h"
#include "storage/tsm_file.h"
#include "struct/Row.h"
#include "io/io_utils.h"
#include "index_manager.h"
#include "struct/Requests.h"

namespace LindormContest {

    class AggregateManager {
    public:
        AggregateManager() = default;

        AggregateManager(uint16_t vin_num, const Path& vin_dir_path, bool finish_compaction, GlobalIndexManagerSPtr index_manager)
                : _vin_num(vin_num), _vin_dir_path(vin_dir_path), _finish_compaction(finish_compaction), _schema(nullptr), _index_manager(index_manager) {}

        AggregateManager(AggregateManager&& other) = default;

        ~AggregateManager() = default;

        void init(SchemaSPtr schema) {
            _schema = schema;
        }

        template<typename T>
        void query_time_range_max_aggregate(const TimeRange& tr, const std::string& column_name, std::vector<Row> &aggregationRes) {
            T max_value = std::numeric_limits<T>::lowest();

            for (uint16_t file_idx = tr._start_idx / FILE_CONVERT_SIZE; file_idx <= tr._end_idx / FILE_CONVERT_SIZE; ++file_idx) {
                TimeRange file_tr (
                        std::max(tr._start_idx, (uint16_t) (file_idx * FILE_CONVERT_SIZE)) % FILE_CONVERT_SIZE,
                        std::min(tr._end_idx, (uint16_t) ((file_idx + 1) * FILE_CONVERT_SIZE - 1)) % FILE_CONVERT_SIZE
                );

                T file_max_value = std::numeric_limits<T>::lowest();

                if (_finish_compaction) {
                    _query_max_from_one_tsm_file<T>(file_idx, file_tr, column_name, file_max_value);
                } else {
                    _query_max_from_one_flush_file<T>(file_idx, file_tr, column_name, file_max_value);
                }

                max_value = std::max(max_value, file_max_value);
            }

            if (unlikely(max_value == std::numeric_limits<T>::lowest())) {
                return;
            }

            ColumnValue max_column_value(max_value);
            Row result_row;
            result_row.columns.emplace(column_name, std::move(max_column_value));
            aggregationRes.emplace_back(std::move(result_row));
        }

        template <typename T>
        void query_time_range_avg_aggregate(const TimeRange& tr, const std::string& column_name, std::vector<Row> &aggregationRes) {
            T sum_value = 0;
            size_t sum_count = 0;

            for (uint16_t file_idx = tr._start_idx / FILE_CONVERT_SIZE; file_idx <= tr._end_idx / FILE_CONVERT_SIZE; ++file_idx) {
                TimeRange file_tr (
                        std::max(tr._start_idx, (uint16_t) (file_idx * FILE_CONVERT_SIZE)) % FILE_CONVERT_SIZE,
                        std::min(tr._end_idx, (uint16_t) ((file_idx + 1) * FILE_CONVERT_SIZE - 1)) % FILE_CONVERT_SIZE
                );

                if (_finish_compaction) {
                    _query_avg_from_one_tsm_file<T>(file_idx, file_tr, column_name, sum_value);
                } else {
                    _query_avg_from_one_flush_file<T>(file_idx, file_tr, column_name, sum_value);
                }

                sum_count += (file_tr._end_idx - file_tr._start_idx + 1);
            }


            if (unlikely(sum_count == 0)) {
                return;
            }

            ColumnValue avg_column_value(sum_value * 1.0 / sum_count);
            Row result_row;
            result_row.columns.emplace(column_name, std::move(avg_column_value));
            aggregationRes.emplace_back(std::move(result_row));
        }

    private:
        template <typename T>
        void _query_max_from_one_tsm_file(uint16_t file_idx, const TimeRange& file_tr,
                                          const std::string& column_name, T& file_max_value) {
            std::vector<IndexEntry> index_entries;
            std::vector<IndexRange> ranges;
            _index_manager->query_indexes(_vin_num, file_idx, column_name, file_tr, index_entries, ranges);
            uint32_t global_offset = index_entries.front()._offset;
            uint32_t global_size = index_entries.back()._offset + index_entries.back()._size - global_offset;
            Path tsm_file_path = _vin_dir_path / std::to_string(file_idx);
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, global_offset, global_size, buf);

            for (size_t i = 0; i < index_entries.size(); ++i) {
                uint32_t local_offset = index_entries[i]._offset - global_offset;
                file_max_value = std::max(file_max_value, _get_max_column_value<T>(buf.c_str() + local_offset, index_entries[i], ranges[i]));
            }
        }

        template <typename T>
        void _query_max_from_one_flush_file(uint16_t file_idx, const TimeRange& file_tr, const std::string& column_name, T& max_value) {
            uint16_t row_nums = file_tr._end_idx - file_tr._start_idx + 1;
            Path flush_file_path = _vin_dir_path / std::to_string(file_idx) / column_name;
            int fd = open(flush_file_path.c_str(), O_RDONLY);
            assert(fd != -1);

            if constexpr (std::is_same_v<T, int32_t>) {
                auto res = lseek(fd, file_tr._start_idx * sizeof(int32_t), SEEK_SET);
                assert(res != -1);
                std::unique_ptr<int32_t[]> int_values = std::make_unique<int32_t[]>(row_nums);
                auto bytes_read = read(fd, int_values.get(), row_nums * sizeof(int32_t));
                assert(bytes_read == row_nums * sizeof(int32_t));
                for (uint16_t i = 0; i < row_nums; ++i) {
                    max_value = std::max(max_value, int_values[i]);
                }
            } else if constexpr (std::is_same_v<T, double_t>) {
                auto res = lseek(fd, file_tr._start_idx * sizeof(double_t), SEEK_SET);
                assert(res != -1);
                std::unique_ptr<double_t[]> double_values = std::make_unique<double_t[]>(row_nums);
                auto bytes_read = read(fd, double_values.get(), row_nums * sizeof(double_t));
                assert(bytes_read == row_nums * sizeof(double_t));
                for (uint16_t i = 0; i < row_nums; ++i) {
                    max_value = std::max(max_value, double_values[i]);
                }
            }

            close(fd);
        }

        template <typename T>
        void _query_avg_from_one_tsm_file(uint16_t file_idx, const TimeRange& file_tr,
                                          const std::string& column_name, T& sum_value) {
            std::vector<IndexEntry> index_entries;
            std::vector<IndexRange> ranges;
            _index_manager->query_indexes(_vin_num, file_idx, column_name, file_tr, index_entries, ranges);
            uint32_t global_offset = index_entries.front()._offset;
            uint32_t global_size = index_entries.back()._offset + index_entries.back()._size - global_offset;
            Path tsm_file_path = _vin_dir_path / std::to_string(file_idx);
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, global_offset, global_size, buf);

            for (size_t i = 0; i < index_entries.size(); ++i) {
                uint32_t local_offset = index_entries[i]._offset - global_offset;
                _get_sum_column_value<T>(buf.c_str() + local_offset, index_entries[i], ranges[i], sum_value);
            }
        }

        template <typename T>
        void _query_avg_from_one_flush_file(uint16_t file_idx, const TimeRange& file_tr,
                                          const std::string& column_name, T& sum_value) {
            uint16_t row_nums = file_tr._end_idx - file_tr._start_idx + 1;
            Path flush_file_path = _vin_dir_path / std::to_string(file_idx) / column_name;
            int fd = open(flush_file_path.c_str(), O_RDONLY);
            assert(fd != -1);

            if constexpr (std::is_same_v<T, int64_t>) {
                auto res = lseek(fd, file_tr._start_idx * sizeof(int32_t), SEEK_SET);
                assert(res != -1);
                std::unique_ptr<int32_t[]> int_values = std::make_unique<int32_t[]>(row_nums);
                auto bytes_read = read(fd, int_values.get(), row_nums * sizeof(int32_t));
                assert(bytes_read == row_nums * sizeof(int32_t));
                for (uint16_t i = 0; i < row_nums; ++i) {
                    sum_value += int_values[i];
                }
            } else if constexpr (std::is_same_v<T, double_t>) {
                auto res = lseek(fd, file_tr._start_idx * sizeof(double_t), SEEK_SET);
                assert(res != -1);
                std::unique_ptr<double_t[]> double_values = std::make_unique<double_t[]>(row_nums);
                auto bytes_read = read(fd, double_values.get(), row_nums * sizeof(double_t));
                assert(bytes_read == row_nums * sizeof(double_t));
                for (uint16_t i = 0; i < row_nums; ++i) {
                    sum_value += double_values[i];
                }
            }

            close(fd);
        }

        template <typename T>
        T _get_max_column_value(const char* buf, const IndexEntry& index_entry, const IndexRange& range) {
            if ((range._end_index - range._start_index + 1) == DATA_BLOCK_ITEM_NUMS) {
                return index_entry.get_max<T>();
            }

            T max_value = std::numeric_limits<T>::lowest();

            if constexpr (std::is_same_v<T, int32_t>) {
                IntDataBlock int_data_block;
                int_data_block.decode_from_decompress(buf);
                for (uint16_t start = range._start_index; start <= range._end_index; ++start) {
                    max_value = std::max(max_value, int_data_block._column_values[start]);
                }
            } else if constexpr (std::is_same_v<T, double_t>) {
                DoubleDataBlock double_data_block;
                double_data_block.decode_from_decompress(buf);
                for (uint16_t start = range._start_index; start <= range._end_index; ++start) {
                    max_value = std::max(max_value, double_data_block._column_values[start]);
                }
            }

            return max_value;
        }

        template <typename T>
        void _get_sum_column_value(const char* buf, const IndexEntry& index_entry,
                                const IndexRange& range, T& sum_value) {
            if ((range._end_index - range._start_index + 1) == DATA_BLOCK_ITEM_NUMS) {
                sum_value += index_entry.get_sum<T>();
                return;
            }

            if constexpr (std::is_same_v<T, int64_t>) {
                IntDataBlock int_data_block;
                int_data_block.decode_from_decompress(buf);
                for (uint16_t start = range._start_index; start <= range._end_index; ++start) {
                    sum_value += int_data_block._column_values[start];
                }
            } else if constexpr (std::is_same_v<T, double_t>) {
                DoubleDataBlock double_data_block;
                double_data_block.decode_from_decompress(buf);
                for (uint16_t start = range._start_index; start <= range._end_index; ++start) {
                    sum_value += double_data_block._column_values[start];
                }
            }
        }

        uint16_t _vin_num;
        Path _vin_dir_path;
        SchemaSPtr _schema;
        bool _finish_compaction;
        GlobalIndexManagerSPtr _index_manager;
    };

    class GlobalAggregateManager;

    using GlobalAggregateManagerUPtr = std::unique_ptr<GlobalAggregateManager>;

    class GlobalAggregateManager {
    public:
        GlobalAggregateManager(const Path& root_path, bool finish_compaction, GlobalIndexManagerSPtr index_manager)
        : _schema(nullptr) {
            for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
                Path vin_dir_path = finish_compaction ?
                                    root_path / "compaction" / std::to_string(vin_num)
                                    : root_path / "no-compaction" / std::to_string(vin_num);
                _agg_managers[vin_num] = std::make_unique<AggregateManager>(vin_num, vin_dir_path, finish_compaction, index_manager);
            }
        }

        ~GlobalAggregateManager() = default;

        void init(SchemaSPtr schema) {
            _schema = schema;
            for (auto &agg_manager: _agg_managers) {
                agg_manager->init(_schema);
            }
        }

        void query_aggregate(uint16_t vin_num, const Vin& vin, int64_t time_lower_inclusive, int64_t time_upper_exclusive,
                             const std::string& column_name, Aggregator aggregator, std::vector<Row>& aggregationRes) {
            TimeRange tr;
            tr.init(time_lower_inclusive, time_upper_exclusive);
            if (unlikely(tr._end_idx >= TS_NUM_RANGE)) {
                return;
            }
            ColumnType type = _schema->columnTypeMap[column_name];
            if (type == COLUMN_TYPE_INTEGER) {
                if (aggregator == MAX) {
                    _agg_managers[vin_num]->query_time_range_max_aggregate<int32_t>(tr, column_name, aggregationRes);
                } else if (aggregator == AVG) {
                    _agg_managers[vin_num]->query_time_range_avg_aggregate<int64_t>(tr, column_name, aggregationRes);
                }
            } else if (type == COLUMN_TYPE_DOUBLE_FLOAT) {
                if (aggregator == MAX) {
                    _agg_managers[vin_num]->query_time_range_max_aggregate<double_t>(tr, column_name, aggregationRes);
                } else if (aggregator == AVG) {
                    _agg_managers[vin_num]->query_time_range_avg_aggregate<double_t>(tr, column_name, aggregationRes);
                }
            }
            aggregationRes[0].vin = vin;
            aggregationRes[0].timestamp = time_lower_inclusive;
        }

    private:
        SchemaSPtr _schema;
        std::unique_ptr<AggregateManager> _agg_managers[VIN_NUM_RANGE];
    };

}