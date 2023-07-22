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

#include "vec/mutable_block.h"

namespace LindormContest::vectorized {

void MutableBlock::insert(size_t position, ColumnPtr column) {
    if (position > data.size()) {
        std::cerr << "Position out of bound in Block::insert(), max position = " << data.size() << std::endl;
    }

    for (auto& name_pos : index_by_name) {
        if (name_pos.second >= position) {
            ++name_pos.second;
        }
    }

    index_by_name.emplace(column->get_name(), position);
    data.emplace(data.begin() + position, column);
}

void MutableBlock::insert(ColumnPtr column) {
    index_by_name.emplace(column->get_name(), data.size());
    data.emplace_back(column);
}

void MutableBlock::erase(const std::set<size_t>& positions) {
    for (auto it = positions.rbegin(); it != positions.rend(); ++it) {
        erase(*it);
    }
}

void MutableBlock::erase(const String& name) {
    auto index_it = index_by_name.find(name);
    if (index_it == index_by_name.end()) {
        std::cerr << "No such name in Block::erase(): " << name << std::endl;
    }

    auto position = index_it->second;
    data.erase(data.begin() + position);

    for (auto it = index_by_name.begin(); it != index_by_name.end(); ++it) {
        if (it->second == position) {
            index_by_name.erase(it++);
        } else if (it->second > position) {
            --it->second;
        } else {
            ++it;
        }
    }
}

Block MutableBlock::to_block(int start_column) {
    return to_block(start_column, data.size());
}

Block MutableBlock::to_block(int start_column, int end_column) {
    Columns columns;
    for (int i = start_column; i < end_column; ++i) {
        columns.emplace_back(static_cast<ColumnPtr>(data[i]));
    }
    return {columns};
}

void MutableBlock::add_row(const Block* block, int row) {
    auto& block_columns = block->get_columns();
    for (int i = 0; i < data.size(); ++i) {
        data[i]->insert_from(*block_columns[i], row);
    }
}

void MutableBlock::add_rows(const Block* block, const int* row_begin, const int* row_end) {
    assert(columns() <= block->columns());
    auto& block_columns = block->get_columns();
    for (int i = 0; i < data.size(); ++i) {
        assert(data[i]->get_type() == block_columns[i]->get_type());
        data[i]->insert_indices_from(*block_columns[i], row_begin, row_end);
    }
}

void MutableBlock::add_rows(const Block* block, size_t row_begin, size_t length) {
    assert(columns() <= block->columns());
    auto& block_columns = block->get_columns();
    for (int i = 0; i < data.size(); ++i) {
        assert(data[i]->get_type() == block_columns[i]->get_type());
        data[i]->insert_range_from(*block_columns[i], row_begin, length);
    }
}

int MutableBlock::compare_one_column(size_t n, size_t m, size_t column_id) const {
    assert(column_id <= columns());
    assert(n <= rows());
    assert(m <= rows());
    auto& column = get_column_by_position(column_id);
    return column->compare_at(n, m, *column);
}

int MutableBlock::compare_at(size_t n, size_t m, size_t num_columns, const MutableBlock& rhs) const {
    assert(columns() >= num_columns);
    assert(rhs.columns() >= num_columns);
    assert(n <= rows());
    assert(n <= rhs.rows());

    for (int i = 0; i < num_columns; ++i) {
        assert(get_type_by_position(i) == rhs.get_type_by_position(i));
        int res = get_column_by_position(i)->compare_at(n, m, *rhs.get_column_by_position(i));
        if (res) {
            return res;
        }
    }

    return 0;
}

int MutableBlock::compare_at(size_t n, size_t m, const std::vector<UInt32>* compare_columns,
               const MutableBlock& rhs) const {
    assert(columns() >= compare_columns->size());
    assert(rhs.columns() >= compare_columns->size());
    assert(n <= rows());
    assert(n <= rhs.rows());

    for (auto i : *compare_columns) {
        assert(get_type_by_position(i) == rhs.get_type_by_position(i));
        int res = get_column_by_position(i)->compare_at(n, m, *rhs.get_column_by_position(i));
        if (res) {
            return res;
        }
    }

    return 0;
}


}
