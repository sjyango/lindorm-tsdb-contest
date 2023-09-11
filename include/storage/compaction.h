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

#include <queue>

#include "Root.h"
#include "storage/tsm_file.h"

namespace LindormContest {

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

    static void multiway_compaction(SchemaSPtr schema, const std::vector<TsmFile>& input_files, TsmFile& output_file) {
        std::map<std::string, std::priority_queue<CompactionRecord, std::vector<CompactionRecord>, CompareFunc>> _min_heaps;

        for (const auto &input_file: input_files) {
            for (const auto &column: schema->columnTypeMap) {
                if (unlikely(_min_heaps.find(column.first) == _min_heaps.cend())) {
                    std::priority_queue<CompactionRecord, std::vector<CompactionRecord>, CompareFunc> _pq;
                    _min_heaps.emplace(column.first, std::move(_pq));
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
                index_entry._max_time_index = end;
                index_entry._count = end - start;
                switch (type) {
                    case COLUMN_TYPE_INTEGER: {
                        int64_t int_sum = 0;
                        for (const auto &v: data_block._column_values) {
                            int32_t int_value;
                            v.getIntegerValue(int_value);
                            int_sum += int_value;
                        }
                        index_entry._sum.emplace<int64_t>(int_sum);
                        break;
                    }
                    case COLUMN_TYPE_DOUBLE_FLOAT: {
                        double_t double_sum = 0.0;
                        for (const auto &v: data_block._column_values) {
                            double_t double_value;
                            v.getDoubleFloatValue(double_value);
                            double_sum += double_value;
                        }
                        index_entry._sum.emplace<double_t>(double_sum);
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

}