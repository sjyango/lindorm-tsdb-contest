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

#include "storage/memmap.h"

namespace LindormContest {

    /// #################### MemMap #####################

    MemMap::MemMap() = default;

    MemMap::~MemMap() = default;

    void MemMap::append(const Row &row) {
        for (auto& column : row.columns) {
            if (_mem_map.find(column.first) == _mem_map.end()) {
                _mem_map[column.first] = {};
            }
            _mem_map[column.first].emplace_back(row.timestamp, column.second);
        }

        _size++;
    }

    bool MemMap::need_flush() const {
        return _size >= MEMMAP_FLUSH_SIZE;
    }

    // flush this mem map into tsm file, but params `offset & size & index offset & footer offset` are empty
    void MemMap::flush_to_tsm_file(SchemaSPtr schema, TsmFile& tsm_file) {
        // encode ts
        for (const auto &item: (*_mem_map.cbegin()).second._values) {
            tsm_file._footer._tss.emplace_back(item.first);
        }
        // encode data blocks and index blocks
        for (const auto &[column_name, column_value]: _mem_map) {
            const size_t value_size = column_value.size();
            ColumnType type = schema->columnTypeMap[column_name];
            IndexBlock index_block(column_name, type);
            std::vector<ColumnValue> column_values;
            column_values.reserve(value_size);

            for (auto item: column_value._values) {
                column_values.emplace_back(std::move(item.second));
            }

            for (size_t start = 0; start < value_size; start += DATA_BLOCK_ITEM_NUMS) {
                size_t end = std::min(start + DATA_BLOCK_ITEM_NUMS, value_size);
                DataBlock data_block(column_values.begin() + start, column_values.begin() + end);
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
                tsm_file._data_blocks.emplace_back(std::move(data_block));
                index_block.add_entry(index_entry); // one index entry corresponds one data block
            }

            tsm_file._index_blocks.emplace_back(std::move(index_block)); // one column data corresponds one index block
        }
    }

    /// #################### MemMap #####################

    /// ################## ShardMemMap ##################

    // ShardMemMap::ShardMemMap() = default;
    //
    // ShardMemMap::~ShardMemMap() = default;
    //
    // void ShardMemMap::set_root_path(const Path &root_path) {
    //     _root_path = root_path;
    // }
    //
    // void ShardMemMap::set_schema(SchemaSPtr schema) {
    //     _schema = schema;
    // }

    // void ShardMemMap::append(const Row &row) {
    //     std::string vin_str(row.vin.vin, VIN_LENGTH);
    //     {
    //         std::lock_guard<SpinLock> l(_mutex);
    //         if (_mem_maps.find(vin_str) == _mem_maps.end()) {
    //             _mem_maps.emplace(vin_str, std::make_unique<MemMap>(_root_path, _schema, vin_str));
    //         }
    //     }
    //     _mem_maps[vin_str]->append(row);
    // }

    /// ################## ShardMemMap ##################
}