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

        AggregateManager(const Vin& vin, const Path& vin_dir_path,
                         SchemaSPtr schema, GlobalIndexManagerSPtr index_manager)
                : _vin(vin), _vin_dir_path(vin_dir_path), _schema(schema), _index_manager(index_manager) {}

        AggregateManager(AggregateManager&& other) = default;

        ~AggregateManager() = default;

        template<typename T>
        void query_time_range_max_aggregate(const TimeRange& tr, const std::string& column_name, std::vector<Row> &aggregationRes) {
            std::string vin_str(_vin.vin, VIN_LENGTH);
            std::vector<std::string> tsm_file_names;
            _get_file_names(tsm_file_names);
            ColumnType type = _schema->columnTypeMap[column_name];
            T max_value = std::numeric_limits<T>::min();

            for (const auto &tsm_file_name: tsm_file_names) {
                T file_max_value = std::numeric_limits<T>::min();
                _query_max_from_one_tsm_file<T>(tsm_file_name, tr, column_name, type, file_max_value);
                max_value = std::max(max_value, file_max_value);
            }

            if (max_value == std::numeric_limits<T>::min()) {
                return;
            }

            ColumnValue max_column_value(max_value);
            Row result_row;
            result_row.vin = _vin;
            result_row.timestamp = tr._start_time;
            result_row.columns.emplace(column_name, std::move(max_column_value));
            aggregationRes.emplace_back(std::move(result_row));
        }

        template <typename T>
        void query_time_range_avg_aggregate(const TimeRange& tr, const std::string& column_name, std::vector<Row> &aggregationRes) {
            std::string vin_str(_vin.vin, VIN_LENGTH);
            std::vector<std::string> tsm_file_names;
            _get_file_names(tsm_file_names);
            ColumnType type = _schema->columnTypeMap[column_name];
            T sum_value = 0;
            size_t sum_count = 0;

            for (const auto &tsm_file_name: tsm_file_names) {
                _query_avg_from_one_tsm_file<T>(tsm_file_name, tr, column_name, type, sum_value, sum_count);
            }

            if (sum_count == 0) {
                return;
            }

            double_t avg_value = sum_value * 1.0 / sum_count;
            ColumnValue avg_column_value(avg_value);
            Row result_row;
            result_row.vin = _vin;
            result_row.timestamp = tr._start_time;
            result_row.columns.emplace(column_name, std::move(avg_column_value));
            aggregationRes.emplace_back(std::move(result_row));
        }

    private:
        template <typename T>
        void _query_max_from_one_tsm_file(const std::string& tsm_file_name, const TimeRange& tr,
                                          const std::string& column_name, ColumnType type, T& max_value) {
            std::string vin_str(_vin.vin, VIN_LENGTH);
            Path tsm_file_path = _vin_dir_path / tsm_file_name;
            Footer footer;
            TsmFile::get_footer(tsm_file_path, footer);
            auto index_entry = _index_manager->query_max_index(vin_str, tsm_file_name, column_name, tr);

            if (!index_entry.has_value()) {
                return;
            }

            std::pair<size_t, size_t> range = _get_value_range(footer._tss, tr, *index_entry);
            max_value = _get_max_column_value<T>(tsm_file_path, type, *index_entry, range);
        }

        template <typename T>
        void _query_avg_from_one_tsm_file(const std::string& tsm_file_name, const TimeRange& tr,
                                          const std::string& column_name, ColumnType type, T& sum_value, size_t& sum_count) {
            std::string vin_str(_vin.vin, VIN_LENGTH);
            Path tsm_file_path = _vin_dir_path / tsm_file_name;
            Footer footer;
            TsmFile::get_footer(tsm_file_path, footer);
            std::vector<IndexEntry> index_entries;
            _index_manager->query_indexes(vin_str, tsm_file_name, column_name, tr, index_entries);

            if (index_entries.empty()) {
                return;
            }

            std::vector<std::pair<size_t, size_t>> ranges;
            _get_value_ranges(footer._tss, tr, index_entries, ranges);

            for (size_t i = 0; i < index_entries.size(); ++i) {
                sum_value += _get_sum_column_value<T>(tsm_file_path, type, index_entries[i], ranges[i]);
            }

            sum_count += (ranges.back().second - ranges.front().first);
        }

        void _get_file_names(std::vector<std::string>& tsm_file_names) {
            for (const auto& entry: std::filesystem::directory_iterator(_vin_dir_path)) {
                if (entry.is_regular_file()) {
                    tsm_file_names.emplace_back(entry.path().filename());
                }
            }
        }

        std::pair<size_t, size_t> _get_value_range(const std::vector<int64_t>& tss,
                                                   const TimeRange& tr, const IndexEntry& index_entry) {
            size_t start = index_entry._min_time_index; // inclusive
            size_t end = index_entry._max_time_index; // inclusive

            while (tss[start] < tr._start_time) { start++; }
            while (tss[end] >= tr._end_time) { end--; }

            return {start, end + 1};
        }

        void _get_value_ranges(const std::vector<int64_t>& tss, const TimeRange& tr,
                               const std::vector<IndexEntry>& index_entries,
                               std::vector<std::pair<size_t, size_t>>& ranges) {
            for (const auto &index_entry: index_entries) {
                ranges.emplace_back(_get_value_range(tss, tr, index_entry));
            }
        }

        template <typename T>
        T _get_max_column_value(const Path& tsm_file_path, ColumnType type,
                                          const IndexEntry& index_entry, const std::pair<size_t, size_t>& range) {
            DataBlock data_block;
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, index_entry._offset, index_entry._size, buf);
            const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.c_str());
            data_block.decode_from(p, type, index_entry._count);
            T max_value = std::numeric_limits<T>::min();

            std::for_each(data_block._column_values.begin() + range.first,
                          data_block._column_values.begin() + range.second,
                          [&max_value] (const ColumnValue& cv) {
                T val;
                if constexpr (std::is_same_v<T, int32_t>) {
                    cv.getIntegerValue(val);
                } else if constexpr (std::is_same_v<T, double_t>) {
                    cv.getDoubleFloatValue(val);
                }
                max_value = std::max(max_value, val);
            });

            return max_value;
        }

        template <typename T>
        T _get_sum_column_value(const Path& tsm_file_path, ColumnType type,
                                const IndexEntry& index_entry, const std::pair<size_t, size_t>& range) {
            DataBlock data_block;
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, index_entry._offset, index_entry._size, buf);
            const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.c_str());
            data_block.decode_from(p, type, index_entry._count);

            if ((range.second - range.first) == index_entry._count) {
                return index_entry.get_sum<T>();
            }

            // TODO(SIMD opt)
            T sum_value = 0;
            std::for_each(data_block._column_values.begin() + range.first,
                          data_block._column_values.begin() + range.second,
                          [&] (const ColumnValue& cv) {
                              if constexpr (std::is_same_v<T, int64_t>) {
                                  int32_t val;
                                  cv.getIntegerValue(val);
                                  sum_value += (int64_t) val;
                              } else if constexpr (std::is_same_v<T, double_t>) {
                                  double_t val;
                                  cv.getDoubleFloatValue(val);
                                  sum_value += val;
                              }
                          });
            return sum_value;
        }

        Vin _vin;
        Path _vin_dir_path;
        SchemaSPtr _schema;
        GlobalIndexManagerSPtr _index_manager;
    };

    class GlobalAggregateManager;

    using GlobalAggregateManagerUPtr = std::unique_ptr<GlobalAggregateManager>;

    class GlobalAggregateManager {
    public:
        GlobalAggregateManager(const Path& root_path, GlobalIndexManagerSPtr index_manager)
                : _root_path(root_path), _index_manager(index_manager) {}

        ~GlobalAggregateManager() = default;

        void set_schema(SchemaSPtr schema) {
            _schema = schema;
        }

        void query_aggregate(const Vin& vin, const std::string& vin_str, int64_t time_lower_inclusive, int64_t time_upper_exclusive,
                             const std::string& column_name, Aggregator aggregator, std::vector<Row>& aggregationRes) {
            Path vin_dir_path = _root_path / vin_str;
            if (unlikely(!std::filesystem::exists(vin_dir_path))) {
                return;
            }
            {
                std::lock_guard<SpinLock> l(_lock);
                if (_agg_managers.find(vin_str) == _agg_managers.end()) {
                    AggregateManager agg_manager(vin, vin_dir_path, _schema, _index_manager);
                    _agg_managers.emplace(vin_str, std::move(agg_manager));
                }
            }
            TimeRange tr = {time_lower_inclusive, time_upper_exclusive};
            ColumnType type = _schema->columnTypeMap[column_name];
            if (type == COLUMN_TYPE_INTEGER) {
                if (aggregator == MAX) {
                    _agg_managers[vin_str].query_time_range_max_aggregate<int32_t>(tr, column_name, aggregationRes);
                } else if (aggregator == AVG) {
                    _agg_managers[vin_str].query_time_range_avg_aggregate<int64_t>(tr, column_name, aggregationRes);
                }
            } else if (type == COLUMN_TYPE_DOUBLE_FLOAT) {
                if (aggregator == MAX) {
                    _agg_managers[vin_str].query_time_range_max_aggregate<double_t>(tr, column_name, aggregationRes);
                } else if (aggregator == AVG) {
                    _agg_managers[vin_str].query_time_range_avg_aggregate<double_t>(tr, column_name, aggregationRes);
                }
            }
        }

    private:
        Path _root_path;
        SpinLock _lock;
        SchemaSPtr _schema;
        GlobalIndexManagerSPtr _index_manager;
        std::unordered_map<std::string, AggregateManager> _agg_managers;
    };

}