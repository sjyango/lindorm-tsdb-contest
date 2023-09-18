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

#include "struct/Vin.h"
#include "storage/tsm_file.h"
#include "struct/Row.h"
#include "io/io_utils.h"
#include "index_manager.h"
#include "common/spinlock.h"

namespace LindormContest {

    class TimeRangeManager {
    public:
        TimeRangeManager() = default;

        TimeRangeManager(const Vin& vin, const Path& vin_dir_path,
                         SchemaSPtr schema, GlobalIndexManagerSPtr index_manager)
        : _vin(vin), _vin_dir_path(vin_dir_path), _schema(schema), _index_manager(index_manager) {}

        TimeRangeManager(TimeRangeManager&& other) = default;

        ~TimeRangeManager() = default;

        void query_time_range(const TimeRange& tr, const std::set<std::string>& requested_columns,
                              std::vector<Row> &trReadRes) {
            std::string vin_str(_vin.vin, VIN_LENGTH);
            std::vector<std::string> tsm_file_names;
            _get_file_names(tsm_file_names);

            for (const auto &tsm_file_name: tsm_file_names) {
                _query_from_one_tsm_file(tsm_file_name, tr, requested_columns, trReadRes);
            }
        }

    private:
        void _query_from_one_tsm_file(const std::string& tsm_file_name, const TimeRange& tr,
                                      const std::set<std::string>& requested_columns, std::vector<Row> &trReadRes) {
            std::string vin_str(_vin.vin, VIN_LENGTH);
            Path tsm_file_path = _vin_dir_path / tsm_file_name;
            std::unordered_map<std::string, std::vector<ColumnValue>> all_column_values;
            Footer footer;
            TsmFile::get_footer(tsm_file_path, footer);
            std::vector<std::pair<size_t, size_t>> ranges;

            for (const auto &column_name: requested_columns) {
                std::vector<IndexEntry> index_entries;
                std::vector<ColumnValue> column_values;
                _index_manager->query_indexes(vin_str, tsm_file_name, column_name, tr, index_entries);

                if (index_entries.empty()) {
                    return;
                }

                if (unlikely(ranges.empty())) {
                    _get_value_ranges(footer._tss, tr, index_entries, ranges);
                }

                _get_column_values(tsm_file_path, _schema->columnTypeMap[column_name], index_entries, ranges, column_values);
                all_column_values.emplace(column_name, std::move(column_values));
            }

            size_t row_nums = all_column_values.begin()->second.size();
            size_t ts_start = ranges.front().first;
            size_t ts_end = ranges.back().second;
            assert(ts_end - ts_start == row_nums);

            for (size_t i = 0; i < row_nums; ++i) {
                Row result_row;
                result_row.vin = _vin;
                result_row.timestamp = footer._tss[ts_start++];

                for (const auto &column_name: requested_columns) {
                    result_row.columns.emplace(column_name, all_column_values[column_name][i]);
                }

                trReadRes.emplace_back(std::move(result_row));
            }

            assert(ts_start == ts_end);
        }

        void _get_file_names(std::vector<std::string>& tsm_file_names) {
            for (const auto& entry: std::filesystem::directory_iterator(_vin_dir_path)) {
                if (entry.is_regular_file()) {
                    tsm_file_names.emplace_back(entry.path().filename());
                }
            }
        }

        void _get_value_ranges(const std::vector<int64_t>& tss, const TimeRange& tr,
                               const std::vector<IndexEntry>& index_entries,
                               std::vector<std::pair<size_t, size_t>>& ranges) {
            for (const auto &index_entry: index_entries) {
                size_t start = index_entry._min_time_index; // inclusive
                size_t end = index_entry._max_time_index; // inclusive

                while (tss[start] < tr._start_time) { start++; }
                while (tss[end] >= tr._end_time) { end--; }

                ranges.emplace_back(start, end + 1);
            }
        }

        void _get_column_values(const Path& tsm_file_path, ColumnType type,
                                const std::vector<IndexEntry>& index_entries,
                                const std::vector<std::pair<size_t, size_t>>& ranges,
                                std::vector<ColumnValue>& column_values) {
            for (size_t i = 0; i < index_entries.size(); ++i) {
                DataBlock data_block;
                std::string buf;
                io::stream_read_string_from_file(tsm_file_path, index_entries[i]._offset, index_entries[i]._size, buf);
                const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.c_str());
                data_block.decode_from(p, type, index_entries[i]._count);
                column_values.insert(column_values.end(),
                                     data_block._column_values.begin() + ranges[i].first,
                                     data_block._column_values.begin() + ranges[i].second);
            }
        }

        Vin _vin;
        Path _vin_dir_path;
        SchemaSPtr _schema;
        GlobalIndexManagerSPtr _index_manager;
    };

    class GlobalTimeRangeManager;

    using GlobalTimeRangeManagerUPtr = std::unique_ptr<GlobalTimeRangeManager>;

    class GlobalTimeRangeManager {
    public:
        GlobalTimeRangeManager(const Path& root_path, GlobalIndexManagerSPtr index_manager)
        : _root_path(root_path), _index_manager(index_manager) {}

        ~GlobalTimeRangeManager() = default;

        void set_schema(SchemaSPtr schema) {
            _schema = schema;
        }

        void query_time_range(const Vin& vin, const std::string& vin_str, int64_t time_lower_inclusive, int64_t time_upper_exclusive,
                              const std::set<std::string>& requested_columns, std::vector<Row> &trReadRes) {
            Path vin_dir_path = _root_path / vin_str;
            if (!std::filesystem::exists(vin_dir_path)) {
                return;
            }
            {
                std::lock_guard<SpinLock> l(_lock);
                if (_tr_managers.find(vin_str) == _tr_managers.end()) {
                    TimeRangeManager tr_manager(vin, vin_dir_path, _schema, _index_manager);
                    _tr_managers.emplace(vin_str, std::move(tr_manager));
                }
            }
            TimeRange tr = {time_lower_inclusive, time_upper_exclusive};
            _tr_managers[vin_str].query_time_range(tr, requested_columns, trReadRes);
        }

    private:
        Path _root_path;
        SpinLock _lock;
        SchemaSPtr _schema;
        GlobalIndexManagerSPtr _index_manager;
        std::unordered_map<std::string, TimeRangeManager> _tr_managers;
    };

}
