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

        TimeRangeManager(uint16_t vin_num, const Path& vin_dir_path, GlobalIndexManagerSPtr index_manager)
        : _vin_num(vin_num), _vin_dir_path(vin_dir_path), _schema(nullptr), _index_manager(index_manager) {}

        TimeRangeManager(TimeRangeManager&& other) = default;

        ~TimeRangeManager() = default;

        void init(SchemaSPtr schema) {
            _schema = schema;
        }

        template <bool finish_compaction>
        void query_time_range(const Vin& vin, const TimeRange& tr, const std::set<std::string>& requested_columns,
                              std::vector<Row> &trReadRes) {
            for (uint16_t file_idx = tr._start_idx / FILE_CONVERT_SIZE; file_idx <= tr._end_idx / FILE_CONVERT_SIZE; ++file_idx) {
                TimeRange file_tr (
                        std::max(tr._start_idx, (uint16_t) (file_idx * FILE_CONVERT_SIZE)) % FILE_CONVERT_SIZE,
                        std::min(tr._end_idx, (uint16_t) ((file_idx + 1) * FILE_CONVERT_SIZE - 1)) % FILE_CONVERT_SIZE
                        );

                if constexpr (finish_compaction) {
                    _query_from_one_tsm_file(vin, file_idx, file_tr, requested_columns, trReadRes);
                } else {
                    _query_from_one_flush_file(vin, file_idx, file_tr, requested_columns, trReadRes);
                }
            }
        }

    private:
        void _query_from_one_tsm_file(const Vin& vin, uint16_t file_idx, const TimeRange& file_tr,
                                      const std::set<std::string>& requested_columns, std::vector<Row> &trReadRes) {
            std::unordered_map<std::string, std::vector<ColumnValue>> all_column_values;

            for (const auto &column_name: requested_columns) {
                std::vector<ColumnValue> column_values;
                std::vector<IndexEntry> index_entries;
                std::vector<IndexRange> ranges;
                _index_manager->query_indexes(_vin_num, file_idx, column_name, file_tr, index_entries, ranges);
                _get_column_values(file_idx, _schema->columnTypeMap[column_name], index_entries, ranges, column_values);
                all_column_values.emplace(column_name, std::move(column_values));
            }

            size_t row_nums = all_column_values.begin()->second.size();
            uint32_t ts_start = file_idx * FILE_CONVERT_SIZE + file_tr._start_idx;
            uint32_t ts_end = file_idx * FILE_CONVERT_SIZE + file_tr._end_idx;

            for (size_t i = 0; i < row_nums; ++i) {
                Row result_row;
                result_row.vin = vin;
                result_row.timestamp = encode_ts(ts_start++);

                for (const auto &column_name: requested_columns) {
                    result_row.columns.emplace(column_name, all_column_values[column_name][i]);
                }

                trReadRes.emplace_back(std::move(result_row));
            }

            assert(ts_start == ts_end + 1);
        }

        void _query_from_one_flush_file(const Vin& vin, uint16_t file_idx, const TimeRange& file_tr,
                                        const std::set<std::string>& requested_columns, std::vector<Row> &trReadRes) {
            Path flush_file_path = _vin_dir_path / std::to_string(file_idx);
            std::string buf;
            io::stream_read_string_from_file(flush_file_path, buf);

            if (buf.empty()) {
                return;
            }

            const char* start = buf.c_str();
            const char* end = start + buf.size();

            while (start < end) {
                Row row;
                io::deserialize_row(_schema, start, false, row);
                uint16_t ts_num = decode_ts(row.timestamp) % FILE_CONVERT_SIZE;
                if (ts_num >= file_tr._start_idx && ts_num <= file_tr._end_idx) {
                    Row result_row;
                    result_row.vin = vin;
                    result_row.timestamp = row.timestamp;
                    for (const auto &requested_column: requested_columns) {
                        result_row.columns.emplace(requested_column, row.columns.at(requested_column));
                    }
                    trReadRes.emplace_back(std::move(result_row));
                }
            }
        }

        void _get_column_values(uint16_t file_idx, ColumnType type,
                                const std::vector<IndexEntry>& index_entries,
                                const std::vector<IndexRange>& ranges,
                                std::vector<ColumnValue>& column_values) {
            Path tsm_file_path = _vin_dir_path / std::to_string(file_idx);
            uint32_t global_offset = index_entries.front()._offset;
            uint32_t global_size = index_entries.back()._offset + index_entries.back()._size - global_offset;
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, global_offset, global_size, buf);

            for (size_t i = 0; i < index_entries.size(); ++i) {
                uint32_t local_offset = index_entries[i]._offset - global_offset;
                switch (type) {
                    case COLUMN_TYPE_INTEGER: {
                        IntDataBlock int_data_block;
                        int_data_block.decode_from_decompress(buf.c_str() + local_offset);
                        for (uint16_t start = ranges[i]._start_index; start <= ranges[i]._end_index; ++start) {
                            column_values.emplace_back(int_data_block._column_values[start]);
                        }
                        break;
                    }
                    case COLUMN_TYPE_DOUBLE_FLOAT: {
                        DoubleDataBlock double_data_block;
                        double_data_block.decode_from_decompress(buf.c_str() + local_offset);
                        for (uint16_t start = ranges[i]._start_index; start <= ranges[i]._end_index; ++start) {
                            column_values.emplace_back(double_data_block._column_values[start]);
                        }
                        break;
                    }
                    case COLUMN_TYPE_STRING: {
                        StringDataBlock str_data_block;
                        str_data_block.decode_from_decompress(buf.c_str() + local_offset);
                        for (uint16_t start = ranges[i]._start_index; start <= ranges[i]._end_index; ++start) {
                            column_values.emplace_back(str_data_block._column_values[start]);
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
        }

        uint16_t _vin_num;
        Path _vin_dir_path;
        SchemaSPtr _schema;
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
                _tr_managers[vin_num] = std::make_unique<TimeRangeManager>(vin_num, vin_dir_path, index_manager);
            }
        }

        ~GlobalTimeRangeManager() = default;

        void init(SchemaSPtr schema) {
            for (auto &tr_manager: _tr_managers) {
                tr_manager->init(schema);
            }
        }

        template <bool finish_compaction>
        void query_time_range(uint16_t vin_num, const Vin& vin, int64_t time_lower_inclusive, int64_t time_upper_exclusive,
                              const std::set<std::string>& requested_columns, std::vector<Row> &trReadRes) {
            TimeRange tr;
            tr.init(time_lower_inclusive, time_upper_exclusive);
            if (unlikely(tr._end_idx >= TS_NUM_RANGE)) {
                return;
            }
            _tr_managers[vin_num]->query_time_range<finish_compaction>(vin, tr, requested_columns, trReadRes);
        }

    private:
        std::unique_ptr<TimeRangeManager> _tr_managers[VIN_NUM_RANGE];
    };

}
