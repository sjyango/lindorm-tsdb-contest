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

#include "Root.h"
#include "common/creator.h"
#include "vec/IColumn.h"
#include "vec/block.h"

namespace LindormContest::vectorized {

class MutableBlock {
    ENABLE_FACTORY_CREATOR(MutableBlock);

    static MutableBlock build_mutable_block(Block* block) {
        return block == nullptr ? MutableBlock() : MutableBlock(block);
    }

    MutableBlock() = default;

    MutableBlock(Block* block) : data {block->mutate_columns()} {
        initialize_index_by_name();
    }

    MutableBlock(Block& block) : data {block.mutate_columns()} {
        initialize_index_by_name();
    }

    MutableBlock(Block&& block) : data {block.mutate_columns()} {
        initialize_index_by_name();
    }

    void initialize_index_by_name() {
        for (size_t i = 0, size = data.size(); i < size; ++i) {
            index_by_name[data[i]->get_name()] = i;
        }
    }

    void reserve(size_t count) {
        index_by_name.reserve(count);
        data.reserve(count);
    }

    size_t rows() const {
        for (const auto& column : data) {
            if (column) {
                return column->size();
            }
        }

        return 0;
    }

    ColumnType get_type_by_position(size_t position) const {
        return data[position]->get_type();
    }

    const MutableColumnPtr& get_column_by_position(size_t position) const {
        return data[position];
    }

    size_t columns() const { return data.size(); }

    bool empty() const { return rows() == 0; }

    void clear() {
        for (auto& column : data) {
            column->clear();
        }
        data.clear();
        index_by_name.clear();
    }

    void insert(size_t position, ColumnPtr column);

    void insert(ColumnPtr column);

    void erase(const std::set<size_t>& positions);

    void erase(const String& name);

    Block to_block(int start_column = 0);

    Block to_block(int start_column, int end_column);

    void add_row(const Block* block, int row);

    void add_rows(const Block* block, const int* row_begin, const int* row_end);

    void add_rows(const Block* block, size_t row_begin, size_t length);

    int compare_one_column(size_t n, size_t m, size_t column_id) const;

    int compare_at(size_t n, size_t m, size_t num_columns, const MutableBlock& rhs) const;

    int compare_at(size_t n, size_t m, const std::vector<UInt32>* compare_columns,
                   const MutableBlock& rhs) const;

private:

    using Container = MutableColumns;
    using IndexByName = std::unordered_map<String, size_t>;

    Container data;
    IndexByName index_by_name;
};

}