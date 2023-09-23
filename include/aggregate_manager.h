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

        void set_schema(SchemaSPtr schema) {
            _schema = schema;
        }

        template<typename T>
        void query_time_range_max_aggregate(const TimeRange& tr, const std::string& column_name, std::vector<Row> &aggregationRes) {
            std::vector<Path> file_paths;
            _get_file_paths(file_paths);
            ColumnType type = _schema->columnTypeMap[column_name];
            T max_value = std::numeric_limits<T>::lowest();

            for (const auto &file_path: file_paths) {
                T file_max_value = std::numeric_limits<T>::lowest();
                if (_finish_compaction) {
                    _query_max_from_one_tsm_file<T>(file_path, tr, column_name, type, file_max_value);
                } else {
                    _query_max_from_one_flush_file<T>(file_path, tr, column_name, file_max_value);
                }
                if (file_max_value == std::numeric_limits<T>::lowest()) {
                    continue;
                }
                max_value = std::max(max_value, file_max_value);
            }

            if (unlikely(max_value == std::numeric_limits<T>::lowest())) {
                return;
            }

            ColumnValue max_column_value(max_value);
            Row result_row;
            result_row.vin = encode_vin(_vin_num);
            result_row.timestamp = tr._start_time;
            result_row.columns.emplace(column_name, std::move(max_column_value));
            aggregationRes.emplace_back(std::move(result_row));
        }

        template <typename T>
        void query_time_range_avg_aggregate(const TimeRange& tr, const std::string& column_name, std::vector<Row> &aggregationRes) {
            std::vector<Path> file_paths;
            _get_file_paths(file_paths);
            ColumnType type = _schema->columnTypeMap[column_name];
            T sum_value = 0;
            size_t sum_count = 0;

            for (const auto &file_path: file_paths) {
                if (_finish_compaction) {
                    _query_avg_from_one_tsm_file<T>(file_path, tr, column_name, type, sum_value, sum_count);
                } else {
                    _query_avg_from_one_flush_file<T>(file_path, tr, column_name, sum_value, sum_count);
                }
            }

            if (unlikely(sum_count == 0)) {
                return;
            }

            double_t avg_value = sum_value * 1.0 / sum_count;
            ColumnValue avg_column_value(avg_value);
            Row result_row;
            result_row.vin = encode_vin(_vin_num);
            result_row.timestamp = tr._start_time;
            result_row.columns.emplace(column_name, std::move(avg_column_value));
            aggregationRes.emplace_back(std::move(result_row));
        }

    private:
        template <typename T>
        void _query_max_from_one_tsm_file(const Path& tsm_file_path, const TimeRange& tr,
                                          const std::string& column_name, ColumnType type, T& max_value) {
            Footer footer;
            TsmFile::get_footer(tsm_file_path, footer);
            std::vector<IndexEntry> index_entries;
            bool existed = _index_manager->query_indexes(_vin_num, tsm_file_path.filename(), column_name, tr, index_entries);

            if (!existed) {
                return;
            }

            std::vector<IndexRange> ranges;
            _get_value_ranges(footer._tss, tr, index_entries, ranges);

            for (size_t i = 0; i < index_entries.size(); ++i) {
                max_value = std::max(max_value, _get_max_column_value<T>(tsm_file_path, type, index_entries[i], ranges[i]));
            }
        }

        template <typename T>
        void _query_max_from_one_flush_file(const Path& flush_file_path, const TimeRange& tr,
                                            const std::string& column_name, T& max_value) {
            std::ifstream input_file;
            input_file.open(flush_file_path, std::ios::in | std::ios::binary);
            assert(input_file.is_open() && input_file.good());

            for (uint16_t i = 0; i < FILE_FLUSH_SIZE && !input_file.eof(); ++i) {
                Row row;
                io::read_row_from_file(input_file, _schema, false, row);
                if (row.timestamp >= tr._start_time && row.timestamp < tr._end_time) {
                    T row_value;
                    if constexpr (std::is_same_v<T, int32_t>) {
                        row.columns.at(column_name).getIntegerValue(row_value);
                    } else if constexpr (std::is_same_v<T, double_t>) {
                        row.columns.at(column_name).getDoubleFloatValue(row_value);
                    }
                    max_value = std::max(max_value, row_value);
                }
            }

            input_file.close();
        }

        template <typename T>
        void _query_avg_from_one_tsm_file(const Path& tsm_file_path, const TimeRange& tr,
                                          const std::string& column_name, ColumnType type, T& sum_value, size_t& sum_count) {
            Footer footer;
            TsmFile::get_footer(tsm_file_path, footer);
            std::vector<IndexEntry> index_entries;
            bool existed = _index_manager->query_indexes(_vin_num, tsm_file_path.filename(), column_name, tr, index_entries);

            if (!existed) {
                return;
            }

            std::vector<IndexRange> ranges;
            _get_value_ranges(footer._tss, tr, index_entries, ranges);

            for (size_t i = 0; i < index_entries.size(); ++i) {
                sum_value += _get_sum_column_value<T>(tsm_file_path, type, index_entries[i], ranges[i]);
            }

            sum_count += (ranges.back().global_end_index() - ranges.front().global_start_index());
        }

        template <typename T>
        void _query_avg_from_one_flush_file(const Path& flush_file_path, const TimeRange& tr,
                                          const std::string& column_name, T& sum_value, size_t& sum_count) {
            std::ifstream input_file;
            input_file.open(flush_file_path, std::ios::in | std::ios::binary);
            assert(input_file.is_open() && input_file.good());

            for (uint16_t i = 0; i < FILE_FLUSH_SIZE && !input_file.eof(); ++i) {
                Row row;
                io::read_row_from_file(input_file, _schema, false, row);
                if (row.timestamp >= tr._start_time && row.timestamp < tr._end_time) {
                    if constexpr (std::is_same_v<T, int64_t>) {
                        int32_t row_value;
                        row.columns.at(column_name).getIntegerValue(row_value);
                        sum_value += (int64_t) row_value;
                    } else if constexpr (std::is_same_v<T, double_t>) {
                        double_t row_value;
                        row.columns.at(column_name).getDoubleFloatValue(row_value);
                        sum_value += row_value;
                    }
                    sum_count++;
                }
            }

            input_file.close();
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
        T _get_max_column_value(const Path& tsm_file_path, ColumnType type,
                                const IndexEntry& index_entry, const IndexRange& range) {
            DataBlock data_block;
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, index_entry._offset, index_entry._size, buf);
            const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.c_str());
            data_block.decode_from(p, type, index_entry._count);

            if ((range._end_index - range._start_index) == index_entry._count) {
                return index_entry.get_max<T>();
            }

            // TODO(SIMD opt)
            T max_value = std::numeric_limits<T>::lowest();
            std::for_each(data_block._column_values.begin() + range._start_index,
                          data_block._column_values.begin() + range._end_index,
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
                                const IndexEntry& index_entry, const IndexRange& range) {
            DataBlock data_block;
            std::string buf;
            io::stream_read_string_from_file(tsm_file_path, index_entry._offset, index_entry._size, buf);
            const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.c_str());
            data_block.decode_from(p, type, index_entry._count);

            if ((range._end_index - range._start_index) == index_entry._count) {
                return index_entry.get_sum<T>();
            }
            // TODO(SIMD opt)
            T sum_value = 0;
            std::for_each(data_block._column_values.begin() + range._start_index,
                          data_block._column_values.begin() + range._end_index,
                          [&sum_value] (const ColumnValue& cv) {
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
                _agg_managers[vin_num] = std::make_unique<AggregateManager>(vin_num, vin_dir_path,
                                                                            finish_compaction, index_manager);
            }
        }

        ~GlobalAggregateManager() = default;

        void set_schema(SchemaSPtr schema) {
            _schema = schema;
            for (auto &agg_manager: _agg_managers) {
                agg_manager->set_schema(_schema);
            }
        }

        void query_aggregate(uint16_t vin_num, int64_t time_lower_inclusive, int64_t time_upper_exclusive,
                             const std::string& column_name, Aggregator aggregator, std::vector<Row>& aggregationRes) {
            TimeRange tr = {time_lower_inclusive, time_upper_exclusive};
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
        }

    private:
        SchemaSPtr _schema;
        std::unique_ptr<AggregateManager> _agg_managers[VIN_NUM_RANGE];
    };

}