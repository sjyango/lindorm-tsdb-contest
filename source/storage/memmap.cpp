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

    MemMap::MemMap(const std::string& vin_str) : _vin_str(vin_str) {
        _cache.reserve(MEMMAP_FLUSH_SIZE);
    }

    MemMap::~MemMap() = default;

    void MemMap::append(const Row &row) {
        _cache.emplace_back(row);
    }

    bool MemMap::empty() const {
        return _cache.empty();
    }

    bool MemMap::need_flush() const {
        return _cache.size() >= MEMMAP_FLUSH_SIZE;
    }

    void MemMap::convert(InternalValue& internal_value) {
        std::sort(_cache.begin(), _cache.end(), [](const Row& lhs, const Row& rhs) {
            return lhs.timestamp < rhs.timestamp;
        });

        for (const auto &row: _cache) {
            internal_value._tss.emplace_back(row.timestamp);

            for (auto& column : row.columns) {
                if (internal_value._values.find(column.first) == internal_value._values.end()) {
                    internal_value._values[column.first] = {};
                }
                internal_value._values[column.first].emplace_back(column.second);
            }
        }
    }

    // flush this mem map into tsm file, but params `offset & size & index offset & footer offset` are empty
    void MemMap::flush_to_tsm_file(SchemaSPtr schema, TsmFile& tsm_file) {
        InternalValue internal_value;
        convert(internal_value);
        // encode ts
        tsm_file._footer._tss = std::move(internal_value._tss);
        // encode data blocks and index blocks
        for (auto& [column_name, column_value]: internal_value._values) {
            const size_t value_size = column_value.size();
            ColumnType type = schema->columnTypeMap[column_name];
            IndexBlock index_block(column_name, type);

            for (size_t start = 0; start < value_size; start += DATA_BLOCK_ITEM_NUMS) {
                size_t end = std::min(start + DATA_BLOCK_ITEM_NUMS, value_size);
                DataBlock data_block(column_value.begin() + start, column_value.begin() + end);
                IndexEntry index_entry;
                index_entry._min_time_index = start;
                index_entry._max_time_index = end - 1;
                index_entry._min_time = tsm_file._footer._tss[index_entry._min_time_index];
                index_entry._max_time = tsm_file._footer._tss[index_entry._max_time_index];
                index_entry._count = end - start;
                switch (type) {
                    case COLUMN_TYPE_INTEGER: {
                        int32_t int_sum = 0;
                        for (const auto &v: data_block._column_values) {
                            int32_t int_value;
                            v.getIntegerValue(int_value);
                            int_sum += int_value;
                        }
                        index_entry._sum.emplace<int32_t>(int_sum);
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