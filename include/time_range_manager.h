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

        TimeRangeManager(uint16_t vin_num, const Path& vin_dir_path,
                         bool finish_compaction, GlobalIndexManagerSPtr index_manager)
        : _vin_num(vin_num), _vin_dir_path(vin_dir_path), _finish_compaction(finish_compaction),
        _schema(nullptr), _index_manager(index_manager) {}

        TimeRangeManager(TimeRangeManager&& other) = default;

        ~TimeRangeManager() = default;

        void set_schema(SchemaSPtr schema) {
            _schema = schema;
        }

        void query_time_range(const TimeRange& tr, const std::set<std::string>& requested_columns,
                              std::vector<Row> &trReadRes) {
            std::vector<Path> file_paths;
            _get_file_paths(file_paths);

            for (const auto &file_path: file_paths) {
                if (_finish_compaction) {
                    _query_from_one_tsm_file(file_path, tr, requested_columns, trReadRes);
                } else {
                    _query_from_one_flush_file(file_path, tr, requested_columns, trReadRes);
                }
            }
        }

    private:
        void _query_from_one_tsm_file(const Path& tsm_file_path, const TimeRange& tr,
                                      const std::set<std::string>& requested_columns, std::vector<Row> &trReadRes) {
            std::unordered_map<std::string, std::vector<ColumnValue>> all_column_values;
            Footer footer;
            TsmFile::get_footer(tsm_file_path, footer);
            std::vector<IndexRange> ranges;

            for (const auto &column_name: requested_columns) {
                std::vector<ColumnValue> column_values;
                std::vector<IndexEntry> index_entries;
                bool existed = _index_manager->query_indexes(_vin_num, tsm_file_path.filename(), column_name, tr, index_entries);

                if (!existed) {
                    return;
                }

                if (unlikely(ranges.empty())) {
                    _get_value_ranges(footer._tss, tr, index_entries, ranges);
                }

                _get_column_values(tsm_file_path, _schema->columnTypeMap[column_name], index_entries, ranges, column_values);
                all_column_values.emplace(column_name, std::move(column_values));
            }

            size_t row_nums = all_column_values.begin()->second.size();
            uint32_t ts_start = ranges.front().global_start_index();
            uint32_t ts_end = ranges.back().global_end_index();

            for (size_t i = 0; i < row_nums; ++i) {
                Row result_row;
                result_row.vin = encode_vin(_vin_num);
                result_row.timestamp = footer._tss[ts_start++];

                for (const auto &column_name: requested_columns) {
                    result_row.columns.emplace(column_name, all_column_values[column_name][i]);
                }

                trReadRes.emplace_back(std::move(result_row));
            }

            assert(ts_start == ts_end);
        }

        void _query_from_one_flush_file(const Path& flush_file_path, const TimeRange& tr,
                                        const std::set<std::string>& requested_columns, std::vector<Row> &trReadRes) {
            std::ifstream input_file;
            input_file.open(flush_file_path, std::ios::in | std::ios::binary);
            if (!input_file.is_open() || !input_file.good()) {
                INFO_LOG("%s open failed", flush_file_path.c_str())
                throw std::runtime_error("time range open file failed");
            }

            while (!input_file.eof()) {
                Row row;
                if (!io::read_row_from_file(input_file, _schema, false, row)) {
                    break;
                }
                if (row.timestamp >= tr._start_time && row.timestamp < tr._end_time) {
                    Row result_row;
                    result_row.vin = encode_vin(_vin_num);
                    result_row.timestamp = row.timestamp;
                    for (const auto &requested_column: requested_columns) {
                        result_row.columns.emplace(requested_column, row.columns.at(requested_column));
                    }
                    trReadRes.emplace_back(std::move(result_row));
                }
            }

            input_file.close();
        }

        void _get_file_paths(std::vector<Path>& file_paths) {
            for (const auto& entry: std::filesystem::directory_iterator(_vin_dir_path)) {
                if (entry.is_regular_file()) {
                    file_paths.emplace_back(entry.path());
                }
            }
        }

        void _get_value_ranges(const std::vector<int64_t>& tss, const TimeRange& tr,
                               const std::vector<IndexEntry>& index_entries,
                               std::vector<IndexRange>& ranges) {
            for (const auto &index_entry: index_entries) {
                size_t start = index_entry._min_time_index; // inclusive
                size_t end = index_entry._max_time_index; // inclusive

                while (tss[start] < tr._start_time) { start++; }
                while (tss[end] >= tr._end_time) { end--; }

                ranges.emplace_back(start % DATA_BLOCK_ITEM_NUMS, end % DATA_BLOCK_ITEM_NUMS + 1, start / DATA_BLOCK_ITEM_NUMS);
            }
        }

        void _get_column_values(const Path& tsm_file_path, ColumnType type,
                                const std::vector<IndexEntry>& index_entries,
                                const std::vector<IndexRange>& ranges,
                                std::vector<ColumnValue>& column_values) {
            for (size_t i = 0; i < index_entries.size(); ++i) {
                DataBlock data_block;
                std::string buf;
                io::stream_read_string_from_file(tsm_file_path, index_entries[i]._offset, index_entries[i]._size, buf);
                const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.c_str());
                data_block.decode_from(p, type, index_entries[i]._count);
                column_values.insert(column_values.end(),
                                     data_block._column_values.begin() + ranges[i]._start_index,
                                     data_block._column_values.begin() + ranges[i]._end_index);
            }
        }

        uint16_t _vin_num;
        Path _vin_dir_path;
        SchemaSPtr _schema;
        bool _finish_compaction;
        GlobalIndexManagerSPtr _index_manager;
    };

    class GlobalTimeRangeManager;

    using GlobalTimeRangeManagerUPtr = std::unique_ptr<GlobalTimeRangeManager>;

    class GlobalTimeRangeManager {
    public:
        GlobalTimeRangeManager(const Path& root_path, bool finish_compaction, GlobalIndexManagerSPtr index_manager) {
            for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
                Path vin_dir_path = finish_compaction ?
                        root_path / "compaction" / std::to_string(vin_num)
                        : root_path / "no-compaction" / std::to_string(vin_num);
                _tr_managers[vin_num] = std::make_unique<TimeRangeManager>(vin_num, vin_dir_path,
                                                                           finish_compaction, index_manager);
            }
        }

        ~GlobalTimeRangeManager() = default;

        void set_schema(SchemaSPtr schema) {
            for (auto &tr_manager: _tr_managers) {
                tr_manager->set_schema(schema);
            }
        }

        void query_time_range(uint16_t vin_num, int64_t time_lower_inclusive, int64_t time_upper_exclusive,
                              const std::set<std::string>& requested_columns, std::vector<Row> &trReadRes) {
            TimeRange tr = {time_lower_inclusive, time_upper_exclusive};
            _tr_managers[vin_num]->query_time_range(tr, requested_columns, trReadRes);
        }

    private:
        std::unique_ptr<TimeRangeManager> _tr_managers[VIN_NUM_RANGE];
    };

}
