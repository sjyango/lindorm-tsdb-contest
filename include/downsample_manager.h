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
#include <numeric>
#include <algorithm>

#include "struct/Vin.h"
#include "struct/Schema.h"
#include "struct/CompareExpression.h"
#include "index_manager.h"
#include "struct/Row.h"
#include "struct/Requests.h"

namespace LindormContest {

    class DownSampleManager {
    public:
        DownSampleManager() = default;

        DownSampleManager(uint16_t vin_num, const Path& vin_dir_path,
                          bool finish_compaction, GlobalIndexManagerSPtr index_manager)
                : _vin_num(vin_num), _vin_dir_path(vin_dir_path), _finish_compaction(finish_compaction),
                _schema(nullptr), _index_manager(index_manager) {}

        DownSampleManager(DownSampleManager&& other) = default;

        ~DownSampleManager() = default;

        void init(SchemaSPtr schema) {
            _schema = schema;
        }

        template <typename T>
        void query_time_range_max_down_sample(int64_t interval, const TimeRange& tr, const std::string& column_name,
                                              const CompareExpression& column_filter, std::vector<Row> &downsampleRes) {
            for (const auto &sub_tr: tr.sub_intervals(interval)) {
                DownSampleState state = DownSampleState::NO_DATA;
                T max_value = std::numeric_limits<T>::lowest();

                // once one tsm has data, the state is HAVE_DATA
                // if all tsms have no data, the state is NO_DATA
                // if some tsms have filtered all data, but the rest tsms have no data, the state is FILTER_ALL_DATA
                for (uint16_t file_idx = sub_tr._start_idx / FILE_CONVERT_SIZE; file_idx <= sub_tr._end_idx / FILE_CONVERT_SIZE; ++file_idx) {
                    TimeRange file_tr (
                            std::max(sub_tr._start_idx, (uint16_t) (file_idx * FILE_CONVERT_SIZE)) % FILE_CONVERT_SIZE,
                            std::min(sub_tr._end_idx, (uint16_t) ((file_idx + 1) * FILE_CONVERT_SIZE - 1)) % FILE_CONVERT_SIZE
                    );

                    T file_max_value = std::numeric_limits<T>::lowest();
                    DownSampleState file_state;

                    if (_finish_compaction) {
                        file_state = _query_max_from_one_tsm_file<T>(file_idx, file_tr, column_name,
                                                                     column_filter, file_max_value);
                    } else {
                        file_state = _query_max_from_one_flush_file<T>(file_idx, file_tr, column_name,
                                                                       column_filter, file_max_value);
                    }

                    if (file_state == DownSampleState::HAVE_DATA) {
                        state = DownSampleState::HAVE_DATA;
                        max_value = std::max(max_value, file_max_value);
                    } else if (state == DownSampleState::NO_DATA && file_state == DownSampleState::FILTER_ALL_DATA) {
                        state = DownSampleState::FILTER_ALL_DATA;
                    }
                }

                if (state == DownSampleState::NO_DATA) {
                    return;
                }

                if (state == DownSampleState::FILTER_ALL_DATA) {
                    if constexpr (std::is_same_v<T, int32_t>) {
                        max_value = INT_NAN;
                    } else if constexpr (std::is_same_v<T, double_t>) {
                        max_value = DOUBLE_NAN;
                    }
                }

                ColumnValue max_column_value(max_value);
                Row result_row;
                result_row.columns.emplace(column_name, std::move(max_column_value));
                downsampleRes.emplace_back(std::move(result_row));
            }
        }

        template <typename T>
        void query_time_range_avg_down_sample(int64_t interval, const TimeRange& tr, const std::string& column_name,
                                              const CompareExpression& column_filter, std::vector<Row> &downsampleRes) {
            for (const auto &sub_tr: tr.sub_intervals(interval)) {
                DownSampleState state = DownSampleState::NO_DATA;
                double_t avg_value;
                T sum_value = 0;
                size_t sum_count = 0;

                // once one tsm has data, the state is HAVE_DATA
                // if all tsms have no data, the state is NO_DATA
                // if some tsms have filtered all data, but the rest tsms have no data, the state is FILTER_ALL_DATA
                for (uint16_t file_idx = sub_tr._start_idx / FILE_CONVERT_SIZE; file_idx <= sub_tr._end_idx / FILE_CONVERT_SIZE; ++file_idx) {
                    TimeRange file_tr (
                            std::max(sub_tr._start_idx, (uint16_t) (file_idx * FILE_CONVERT_SIZE)) % FILE_CONVERT_SIZE,
                            std::min(sub_tr._end_idx, (uint16_t) ((file_idx + 1) * FILE_CONVERT_SIZE - 1)) % FILE_CONVERT_SIZE
                    );

                    T file_sum_value = 0;
                    size_t file_sum_count = 0;
                    DownSampleState file_state;

                    if (_finish_compaction) {
                        file_state = _query_avg_from_one_tsm_file<T>(file_idx, file_tr, column_name,
                                                                     column_filter, file_sum_value, file_sum_count);
                    } else {
                        file_state = _query_avg_from_one_flush_file<T>(file_idx, file_tr, column_name,
                                                                       column_filter, file_sum_value, file_sum_count);
                    }

                    if (file_state == DownSampleState::HAVE_DATA) {
                        state = DownSampleState::HAVE_DATA;
                        sum_value += file_sum_value;
                        sum_count += file_sum_count;
                    } else if (state == DownSampleState::NO_DATA && file_state == DownSampleState::FILTER_ALL_DATA) {
                        state = DownSampleState::FILTER_ALL_DATA;
                    }
                }

                if (state == DownSampleState::NO_DATA) {
                    return;
                }

                if (state == DownSampleState::FILTER_ALL_DATA) {
                    avg_value = DOUBLE_NAN;
                } else {
                    avg_value = sum_value * 1.0 / sum_count;
                }

                ColumnValue max_column_value(avg_value);
                Row result_row;
                result_row.columns.emplace(column_name, std::move(max_column_value));
                downsampleRes.emplace_back(std::move(result_row));
            }
        }

    private:
        enum class DownSampleState {
            HAVE_DATA,
            NO_DATA,
            FILTER_ALL_DATA
        };

        template <typename T>
        DownSampleState _query_max_from_one_tsm_file(uint16_t file_idx, const TimeRange& file_tr, const std::string& column_name,
                                                     const CompareExpression& column_filter, T& max_value) {
            std::vector<IndexEntry> index_entries;
            std::vector<IndexRange> ranges;
            _index_manager->query_indexes(_vin_num, file_idx, column_name, file_tr, index_entries, ranges);
            DownSampleState file_state = DownSampleState::NO_DATA;
            uint32_t global_offset = index_entries.front()._offset;
            uint32_t global_size = index_entries.back()._offset + index_entries.back()._size - global_offset;
            Path tsm_file_path = _vin_dir_path / std::to_string(file_idx);
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, global_offset, global_size, buf);

            for (size_t i = 0; i < index_entries.size(); ++i) {
                DownSampleState entry_state;
                T entry_max_value = std::numeric_limits<T>::lowest();
                uint32_t local_offset = index_entries[i]._offset - global_offset;
                entry_state = _get_max_column_value<T>(buf.c_str() + local_offset,
                                                       index_entries[i], column_filter,
                                                       ranges[i], entry_max_value);
                if (entry_state == DownSampleState::HAVE_DATA) {
                    file_state = DownSampleState::HAVE_DATA;
                    max_value = std::max(max_value, entry_max_value);
                } else if (file_state == DownSampleState::NO_DATA && entry_state == DownSampleState::FILTER_ALL_DATA) {
                    file_state = DownSampleState::FILTER_ALL_DATA;
                }
            }

            return file_state;
        }

        template <typename T>
        DownSampleState _query_max_from_one_flush_file(uint16_t file_idx, const TimeRange& file_tr, const std::string& column_name,
                                                       const CompareExpression& column_filter, T& max_value) {
            uint16_t row_nums = file_tr._end_idx - file_tr._start_idx + 1;
            Path flush_file_path = _vin_dir_path / std::to_string(file_idx) / column_name;
            int fd = open(flush_file_path.c_str(), O_RDONLY);
            assert(fd != -1);
            size_t tr_count = 0;
            size_t non_filter_count = 0;

            if constexpr (std::is_same_v<T, int32_t>) {
                auto res = lseek(fd, file_tr._start_idx * sizeof(int32_t), SEEK_SET);
                assert(res != -1);
                std::unique_ptr<int32_t[]> int_values = std::make_unique<int32_t[]>(row_nums);
                auto bytes_read = read(fd, int_values.get(), row_nums * sizeof(int32_t));
                assert(bytes_read == row_nums * sizeof(int32_t));
                for (uint16_t i = 0; i < row_nums; ++i) {
                    tr_count++;
                    if (!column_filter.doCompare(ColumnValue(int_values[i]))) {
                        continue;
                    }
                    non_filter_count++;
                    max_value = std::max(max_value, int_values[i]);
                }
            } else if constexpr (std::is_same_v<T, double_t>) {
                auto res = lseek(fd, file_tr._start_idx * sizeof(double_t), SEEK_SET);
                assert(res != -1);
                std::unique_ptr<double_t[]> double_values = std::make_unique<double_t[]>(row_nums);
                auto bytes_read = read(fd, double_values.get(), row_nums * sizeof(double_t));
                assert(bytes_read == row_nums * sizeof(double_t));
                for (uint16_t i = 0; i < row_nums; ++i) {
                    tr_count++;
                    if (!column_filter.doCompare(ColumnValue(double_values[i]))) {
                        continue;
                    }
                    non_filter_count++;
                    max_value = std::max(max_value, double_values[i]);
                }
            }

            close(fd);

            if (tr_count == 0) {
                return DownSampleState::NO_DATA;
            } else if (non_filter_count == 0) {
                return DownSampleState::FILTER_ALL_DATA;
            } else {
                return DownSampleState::HAVE_DATA;
            }
        }

        template <typename T>
        DownSampleState _query_avg_from_one_tsm_file(uint16_t file_idx, const TimeRange& file_tr,
                                            const std::string& column_name, const CompareExpression& column_filter,
                                            T& sum_value, size_t& sum_count) {
            std::vector<IndexEntry> index_entries;
            std::vector<IndexRange> ranges;
            _index_manager->query_indexes(_vin_num, file_idx, column_name, file_tr, index_entries, ranges);
            DownSampleState file_state = DownSampleState::NO_DATA;
            uint32_t global_offset = index_entries.front()._offset;
            uint32_t global_size = index_entries.back()._offset + index_entries.back()._size - global_offset;
            Path tsm_file_path = _vin_dir_path / std::to_string(file_idx);
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, global_offset, global_size, buf);

            for (size_t i = 0; i < index_entries.size(); ++i) {
                DownSampleState entry_state;
                T entry_sum_value = 0;
                size_t entry_sum_count = 0;
                uint32_t local_offset = index_entries[i]._offset - global_offset;
                entry_state = _get_sum_column_value<T>(buf.c_str() + local_offset,
                                                       index_entries[i], column_filter,
                                                       ranges[i], entry_sum_value, entry_sum_count);
                if (entry_state == DownSampleState::HAVE_DATA) {
                    file_state = DownSampleState::HAVE_DATA;
                    sum_value += entry_sum_value;
                    sum_count += entry_sum_count;
                } else if (file_state == DownSampleState::NO_DATA && entry_state == DownSampleState::FILTER_ALL_DATA) {
                    file_state = DownSampleState::FILTER_ALL_DATA;
                }
            }

            return file_state;
        }

        template <typename T>
        DownSampleState _query_avg_from_one_flush_file(uint16_t file_idx, const TimeRange& file_tr, const std::string& column_name,
                                                     const CompareExpression& column_filter, T& sum_value, size_t& sum_count) {
            uint16_t row_nums = file_tr._end_idx - file_tr._start_idx + 1;
            Path flush_file_path = _vin_dir_path / std::to_string(file_idx) / column_name;
            int fd = open(flush_file_path.c_str(), O_RDONLY);
            assert(fd != -1);
            size_t tr_count = 0;
            size_t non_filter_count = 0;

            if constexpr (std::is_same_v<T, int64_t>) {
                auto res = lseek(fd, file_tr._start_idx * sizeof(int32_t), SEEK_SET);
                assert(res != -1);
                std::unique_ptr<int32_t[]> int_values = std::make_unique<int32_t[]>(row_nums);
                auto bytes_read = read(fd, int_values.get(), row_nums * sizeof(int32_t));
                assert(bytes_read == row_nums * sizeof(int32_t));
                for (uint16_t i = 0; i < row_nums; ++i) {
                    tr_count++;
                    if (!column_filter.doCompare(ColumnValue(int_values[i]))) {
                        continue;
                    }
                    non_filter_count++;
                    sum_value += int_values[i];
                    sum_count++;
                }
            } else if constexpr (std::is_same_v<T, double_t>) {
                auto res = lseek(fd, file_tr._start_idx * sizeof(double_t), SEEK_SET);
                assert(res != -1);
                std::unique_ptr<double_t[]> double_values = std::make_unique<double_t[]>(row_nums);
                auto bytes_read = read(fd, double_values.get(), row_nums * sizeof(double_t));
                assert(bytes_read == row_nums * sizeof(double_t));
                for (uint16_t i = 0; i < row_nums; ++i) {
                    tr_count++;
                    if (!column_filter.doCompare(ColumnValue(double_values[i]))) {
                        continue;
                    }
                    non_filter_count++;
                    sum_value += double_values[i];
                    sum_count++;
                }
            }

            close(fd);

            if (tr_count == 0) {
                return DownSampleState::NO_DATA;
            } else if (non_filter_count == 0) {
                return DownSampleState::FILTER_ALL_DATA;
            } else {
                return DownSampleState::HAVE_DATA;
            }
        }

        template <typename T>
        DownSampleState _get_max_column_value(const char* buf, const IndexEntry& index_entry,
                                const CompareExpression& column_filter, const IndexRange& range, T& max_value) {
            if constexpr (std::is_same_v<T, int32_t>) {
                IntDataBlock int_data_block;
                int_data_block.decode_from_decompress(buf);
                for (uint16_t start = range._start_index; start <= range._end_index; ++start) {
                    ColumnValue cv(int_data_block._column_values[start]);
                    if (!column_filter.doCompare(cv)) {
                        continue;
                    }
                    max_value = std::max(max_value, int_data_block._column_values[start]);
                }
            } else if constexpr (std::is_same_v<T, double_t>) {
                DoubleDataBlock double_data_block;
                double_data_block.decode_from_decompress(buf);
                for (uint16_t start = range._start_index; start <= range._end_index; ++start) {
                    ColumnValue cv(double_data_block._column_values[start]);
                    if (!column_filter.doCompare(cv)) {
                        continue;
                    }
                    max_value = std::max(max_value, double_data_block._column_values[start]);
                }
            }

            if (max_value == std::numeric_limits<T>::lowest()) {
                return DownSampleState::FILTER_ALL_DATA;
            }

            return DownSampleState::HAVE_DATA;
        }

        template <typename T>
        DownSampleState _get_sum_column_value(const char* buf, const IndexEntry& index_entry,
                                              const CompareExpression& column_filter, const IndexRange& range,
                                              T& sum_value, size_t& sum_count) {
            if constexpr (std::is_same_v<T, int64_t>) {
                IntDataBlock int_data_block;
                int_data_block.decode_from_decompress(buf);
                for (uint16_t start = range._start_index; start <= range._end_index; ++start) {
                    ColumnValue cv(int_data_block._column_values[start]);
                    if (!column_filter.doCompare(cv)) {
                        continue;
                    }
                    sum_value += int_data_block._column_values[start];
                    sum_count++;
                }
            } else if constexpr (std::is_same_v<T, double_t>) {
                DoubleDataBlock double_data_block;
                double_data_block.decode_from_decompress(buf);
                for (uint16_t start = range._start_index; start <= range._end_index; ++start) {
                    ColumnValue cv(double_data_block._column_values[start]);
                    if (!column_filter.doCompare(cv)) {
                        continue;
                    }
                    sum_value += double_data_block._column_values[start];
                    sum_count++;
                }
            }

            if (sum_count == 0) {
                return DownSampleState::FILTER_ALL_DATA;
            }

            return DownSampleState::HAVE_DATA;
        }

        uint16_t _vin_num;
        Path _vin_dir_path;
        SchemaSPtr _schema;
        bool _finish_compaction;
        GlobalIndexManagerSPtr _index_manager;
    };

    class GlobalDownSampleManager;

    using GlobalDownSampleManagerUPtr = std::unique_ptr<GlobalDownSampleManager>;

    class GlobalDownSampleManager {
    public:
        GlobalDownSampleManager(const Path& root_path, bool finish_compaction, GlobalIndexManagerSPtr index_manager)
        : _schema(nullptr) {
            for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
                Path vin_dir_path = finish_compaction ?
                                    root_path / "compaction" / std::to_string(vin_num)
                                    : root_path / "no-compaction" / std::to_string(vin_num);
                _ds_managers[vin_num] = std::make_unique<DownSampleManager>(vin_num, vin_dir_path,
                                                                            finish_compaction, index_manager);
            }
        }

        ~GlobalDownSampleManager() = default;

        void init(SchemaSPtr schema) {
            _schema = schema;
            for (auto &ds_manager: _ds_managers) {
                ds_manager->init(_schema);
            }
        }

        void query_down_sample(uint16_t vin_num, const Vin& vin, int64_t time_lower_inclusive, int64_t time_upper_exclusive,
                               int64_t interval, const std::string& column_name, Aggregator aggregator,
                               const CompareExpression& columnFilter, std::vector<Row>& downsampleRes) {
            TimeRange tr;
            tr.init(time_lower_inclusive, time_upper_exclusive);
            if (unlikely(tr._end_idx >= TS_NUM_RANGE)) {
                return;
            }
            ColumnType type = _schema->columnTypeMap[column_name];
            if (type == COLUMN_TYPE_INTEGER) {
                if (aggregator == MAX) {
                    _ds_managers[vin_num]->query_time_range_max_down_sample<int32_t>(interval, tr, column_name, columnFilter, downsampleRes);
                } else if (aggregator == AVG) {
                    _ds_managers[vin_num]->query_time_range_avg_down_sample<int64_t>(interval, tr, column_name, columnFilter, downsampleRes);
                }
            } else if (type == COLUMN_TYPE_DOUBLE_FLOAT) {
                if (aggregator == MAX) {
                    _ds_managers[vin_num]->query_time_range_max_down_sample<double_t>(interval, tr, column_name, columnFilter, downsampleRes);
                } else if (aggregator == AVG) {
                    _ds_managers[vin_num]->query_time_range_avg_down_sample<double_t>(interval, tr, column_name, columnFilter, downsampleRes);
                }
            }

            for (uint32_t i = 0; i < downsampleRes.size(); ++i) {
                downsampleRes[i].vin = vin;
                downsampleRes[i].timestamp = time_lower_inclusive + i * interval;
            }
        }

    private:
        SchemaSPtr _schema;
        std::unique_ptr<DownSampleManager> _ds_managers[VIN_NUM_RANGE];
    };
}