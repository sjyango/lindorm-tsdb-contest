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

        DownSampleManager(uint16_t vin_num, const Path& vin_dir_path, GlobalIndexManagerSPtr index_manager)
                : _vin_num(vin_num), _vin_dir_path(vin_dir_path), _schema(nullptr), _index_manager(index_manager) {}

        DownSampleManager(DownSampleManager&& other) = default;

        ~DownSampleManager() = default;

        void set_schema(SchemaSPtr schema) {
            _schema = schema;
        }

        template <typename T>
        void query_time_range_max_down_sample(int64_t interval, const TimeRange& tr, const std::string& column_name,
                                              const CompareExpression& column_filter, std::vector<Row> &downsampleRes) {
            std::vector<Path> tsm_file_paths;
            _get_file_paths(tsm_file_paths);
            ColumnType type = _schema->columnTypeMap[column_name];

            for (const auto &sub_tr: tr.sub_intervals(interval)) {
                DownSampleState state = DownSampleState::NO_DATA;
                T max_value = std::numeric_limits<T>::lowest();

                // once one tsm has data, the state is HAVE_DATA
                // if all tsms have no data, the state is NO_DATA
                // if some tsms have filtered all data, but the rest tsms have no data, the state is FILTER_ALL_DATA
                for (const auto &tsm_file_path: tsm_file_paths) {
                    T file_max_value = std::numeric_limits<T>::lowest();
                    DownSampleState file_state = _query_max_from_one_tsm_file<T>(tsm_file_path, sub_tr, column_name,
                                                                      type, column_filter, file_max_value);
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
                result_row.vin = encode_vin(_vin_num);
                result_row.timestamp = sub_tr._start_time;
                result_row.columns.emplace(column_name, std::move(max_column_value));
                downsampleRes.emplace_back(std::move(result_row));
            }
        }

        template <typename T>
        void query_time_range_avg_down_sample(int64_t interval, const TimeRange& tr, const std::string& column_name,
                                              const CompareExpression& column_filter, std::vector<Row> &downsampleRes) {
            std::vector<Path> tsm_file_paths;
            _get_file_paths(tsm_file_paths);
            ColumnType type = _schema->columnTypeMap[column_name];

            for (const auto &sub_tr: tr.sub_intervals(interval)) {
                DownSampleState state = DownSampleState::NO_DATA;
                double_t avg_value;
                T sum_value = 0;
                size_t sum_count = 0;

                // once one tsm has data, the state is HAVE_DATA
                // if all tsms have no data, the state is NO_DATA
                // if some tsms have filtered all data, but the rest tsms have no data, the state is FILTER_ALL_DATA
                for (const auto &tsm_file_path: tsm_file_paths) {
                    T file_sum_value = 0;
                    size_t file_sum_count = 0;
                    DownSampleState file_state = _query_avg_from_one_tsm_file<T>(tsm_file_path, sub_tr, column_name, type,
                                                                                 column_filter, file_sum_value, file_sum_count);
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
                result_row.vin = encode_vin(_vin_num);
                result_row.timestamp = sub_tr._start_time;
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
        DownSampleState _query_max_from_one_tsm_file(const Path& tsm_file_path, const TimeRange& tr,
                                          const std::string& column_name, ColumnType type,
                                          const CompareExpression& column_filter, T& max_value) {
            Footer footer;
            TsmFile::get_footer(tsm_file_path, footer);
            std::vector<IndexEntry> index_entries;
            bool existed = _index_manager->query_indexes(_vin_num, tsm_file_path.filename(), column_name, tr, index_entries);

            if (!existed) {
                return DownSampleState::NO_DATA;
            }

            DownSampleState file_state = DownSampleState::NO_DATA;
            std::vector<IndexRange> ranges;
            _get_value_ranges(footer._tss, tr, index_entries, ranges);

            for (size_t i = 0; i < index_entries.size(); ++i) {
                DownSampleState entry_state;
                T entry_max_value;
                entry_state = _get_max_column_value<T>(tsm_file_path, type, index_entries[i], column_filter,
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
        DownSampleState _query_avg_from_one_tsm_file(const Path& tsm_file_path, const TimeRange& tr,
                                            const std::string& column_name, ColumnType type,
                                            const CompareExpression& column_filter, T& sum_value, size_t& sum_count) {
            Footer footer;
            TsmFile::get_footer(tsm_file_path, footer);
            std::vector<IndexEntry> index_entries;
            bool existed = _index_manager->query_indexes(_vin_num, tsm_file_path.filename(), column_name, tr, index_entries);

            if (!existed) {
                return DownSampleState::NO_DATA;
            }

            DownSampleState file_state = DownSampleState::NO_DATA;
            std::vector<IndexRange> ranges;
            _get_value_ranges(footer._tss, tr, index_entries, ranges);

            for (size_t i = 0; i < index_entries.size(); ++i) {
                DownSampleState entry_state;
                T entry_sum_value = 0;
                size_t entry_sum_count = 0;
                entry_state = _get_sum_column_value<T>(tsm_file_path, type, index_entries[i], column_filter,
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

        void _get_file_paths(std::vector<Path>& tsm_file_paths) {
            for (const auto& entry: std::filesystem::directory_iterator(_vin_dir_path)) {
                if (entry.is_regular_file()) {
                    tsm_file_paths.emplace_back(entry.path());
                }
            }
        }

        IndexRange _get_value_range(const std::vector<int64_t>& tss, const TimeRange& tr, const IndexEntry& index_entry) {
            uint16_t start = index_entry._min_time_index; // inclusive
            uint16_t end = index_entry._max_time_index; // inclusive

            while (tss[start] < tr._start_time) { start++; }
            while (tss[end] >= tr._end_time) { end--; }

            return {static_cast<uint16_t>(start % DATA_BLOCK_ITEM_NUMS),
                    static_cast<uint16_t>(end % DATA_BLOCK_ITEM_NUMS + 1),
                    static_cast<uint16_t>(start / DATA_BLOCK_ITEM_NUMS)};
        }

        void _get_value_ranges(const std::vector<int64_t>& tss, const TimeRange& tr,
                               const std::vector<IndexEntry>& index_entries,
                               std::vector<IndexRange>& ranges) {
            for (const auto &index_entry: index_entries) {
                ranges.emplace_back(_get_value_range(tss, tr, index_entry));
            }
        }

        template <typename T>
        DownSampleState _get_max_column_value(const Path& tsm_file_path, ColumnType type, const IndexEntry& index_entry,
                                const CompareExpression& column_filter, const IndexRange& range, T& max_value) {
            if (index_entry._size == 0 || index_entry._count == 0) {
                return DownSampleState::NO_DATA;
            }

            DataBlock data_block;
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, index_entry._offset, index_entry._size, buf);
            const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.c_str());
            data_block.decode_from(p, type, index_entry._count);
            max_value = std::numeric_limits<T>::lowest();

            std::for_each(data_block._column_values.begin() + range._start_index,
                          data_block._column_values.begin() + range._end_index,
                          [&] (const ColumnValue& cv) {
                              if (!column_filter.doCompare(cv)) {
                                  return;
                              }
                              T val;
                              if constexpr (std::is_same_v<T, int32_t>) {
                                  cv.getIntegerValue(val);
                              } else if constexpr (std::is_same_v<T, double_t>) {
                                  cv.getDoubleFloatValue(val);
                              }
                              max_value = std::max(max_value, val);
                          });

            if (max_value == std::numeric_limits<T>::lowest()) {
                return DownSampleState::FILTER_ALL_DATA;
            }

            return DownSampleState::HAVE_DATA;
        }

        template <typename T>
        DownSampleState _get_sum_column_value(const Path& tsm_file_path, ColumnType type, const IndexEntry& index_entry,
                                              const CompareExpression& column_filter, const IndexRange& range,
                                              T& sum_value, size_t& sum_count) {
            if (index_entry._size == 0 || index_entry._count == 0) {
                return DownSampleState::NO_DATA;
            }

            DataBlock data_block;
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, index_entry._offset, index_entry._size, buf);
            const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.c_str());
            data_block.decode_from(p, type, index_entry._count);

            std::for_each(data_block._column_values.begin() + range._start_index,
                          data_block._column_values.begin() + range._end_index,
                          [&] (const ColumnValue& cv) {
                              if (!column_filter.doCompare(cv)) {
                                  return;
                              }
                              if constexpr (std::is_same_v<T, int64_t>) {
                                  int32_t val;
                                  cv.getIntegerValue(val);
                                  sum_value += (int64_t) val;
                              } else if constexpr (std::is_same_v<T, double_t>) {
                                  double_t val;
                                  cv.getDoubleFloatValue(val);
                                  sum_value += val;
                              }
                              sum_count++;
                          });

            if (sum_count == 0) {
                return DownSampleState::FILTER_ALL_DATA;
            }

            return DownSampleState::HAVE_DATA;
        }

        uint16_t _vin_num;
        Path _vin_dir_path;
        SchemaSPtr _schema;
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
                _ds_managers[vin_num] = std::make_unique<DownSampleManager>(vin_num, vin_dir_path, index_manager);
            }
        }

        ~GlobalDownSampleManager() = default;

        void set_schema(SchemaSPtr schema) {
            _schema = schema;
            for (auto &ds_manager: _ds_managers) {
                ds_manager->set_schema(_schema);
            }
        }

        void query_down_sample(uint16_t vin_num, int64_t time_lower_inclusive, int64_t time_upper_exclusive,
                               int64_t interval, const std::string& column_name, Aggregator aggregator,
                               const CompareExpression& columnFilter, std::vector<Row>& downsampleRes) {
            TimeRange tr = {time_lower_inclusive, time_upper_exclusive};
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
        }

    private:
        SchemaSPtr _schema;
        std::unique_ptr<DownSampleManager> _ds_managers[VIN_NUM_RANGE];
    };
}