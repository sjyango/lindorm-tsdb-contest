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

#include "vec/blocks/mutable_block.h"

namespace LindormContest::vectorized {


void MutableBlock::erase(const String& name) {
    auto index_it = _index_by_name.find(name);
    if (index_it == _index_by_name.end()) {
        std::cerr << "No such name in Block::erase(): " << name << std::endl;
    }

    auto position = index_it->second;
    _data.erase(_data.begin() + position);

    for (auto it = _index_by_name.begin(); it != _index_by_name.end(); ++it) {
        if (it->second == position) {
            _index_by_name.erase(it++);
        } else if (it->second > position) {
            --it->second;
        } else {
            ++it;
        }
    }
}

Block MutableBlock::to_block(int start_column) {
    return to_block(start_column, _data.size());
}

Block MutableBlock::to_block(int start_column, int end_column) {
    ColumnsWithTypeAndName columns_with_type_and_name;
    for (int i = start_column; i < end_column; ++i) {
        columns_with_type_and_name.emplace_back(std::move(_data[i]), _data_types[i], _names[i]);
    }
    return {columns_with_type_and_name};
}

void MutableBlock::add_row(const Block* block, size_t row) {
    auto block_columns = block->get_columns();
    for (int i = 0; i < _data.size(); ++i) {
        _data[i]->insert_from(*block_columns[i], row);
    }
}

void MutableBlock::add_row(const Row& row) {
    assert(_data.size() == row.columns.size() + 2);
    ColumnString& vin_col = reinterpret_cast<ColumnString&>(*_data[_index_by_name["vin"]]);
    vin_col.push_string(row.vin.vin, 17);
    ColumnInt64& timestamp_col = reinterpret_cast<ColumnInt64&>(*_data[_index_by_name["timestamp"]]);
    timestamp_col.push_number(row.timestamp);

    for (const auto& pair : row.columns) {
        // KEY: columnFieldName, VALVE: column data.
        assert(_index_by_name.count(pair.first) != 0);
        int res;
        switch (pair.second.columnType) {
        case COLUMN_TYPE_INTEGER: {
            ColumnInt32& int_col = reinterpret_cast<ColumnInt32&>(*_data[_index_by_name[pair.first]]);
            Int32 int_val;
            res = pair.second.getIntegerValue(int_val);
            int_col.push_number(int_val);
            break;
        }
        case COLUMN_TYPE_DOUBLE_FLOAT: {
            ColumnFloat64& double_col = reinterpret_cast<ColumnFloat64&>(*_data[_index_by_name[pair.first]]);
            Float64 double_val;
            res = pair.second.getDoubleFloatValue(double_val);
            double_col.push_number(double_val);
            break;
        }
        case COLUMN_TYPE_STRING: {
            ColumnString& str_col = reinterpret_cast<ColumnString&>(*_data[_index_by_name[pair.first]]);
            std::pair<int32_t, const char *> str_val;
            res = pair.second.getStringValue(str_val);
            str_col.push_string(str_val.second, str_val.first);
            break;
        }
        default: {
            res = 1;
        }
        }
        assert(res == 0);
    }
}

void MutableBlock::append_block(const Block* block, const size_t* row_begin, const size_t* row_end) {
    assert(columns() <= block->columns());
    auto block_columns = block->get_columns();
    for (int i = 0; i < _data.size(); ++i) {
        assert(_data[i]->get_type() == block_columns[i]->get_type());
        _data[i]->insert_indices_from(*block_columns[i], row_begin, row_end);
    }
}

void MutableBlock::append_block(const Block* block, size_t row_begin, size_t length) {
    assert(columns() <= block->columns());
    auto block_columns = block->get_columns();
    for (int i = 0; i < _data.size(); ++i) {
        assert(_data[i]->get_type() == block_columns[i]->get_type());
        _data[i]->insert_range_from(*block_columns[i], row_begin, length);
    }
}

int MutableBlock::compare_one_column(size_t n, size_t m, size_t column_id) const {
    assert(column_id <= columns());
    assert(n <= rows());
    assert(m <= rows());
    MutableColumnSPtr column = get_column_by_position(column_id);
    return column->compare_at(n, m, *column);
}

int MutableBlock::compare_at(size_t n, size_t m, size_t num_columns, const MutableBlock& rhs) const {
    assert(num_columns <= columns());
    assert(num_columns <= rhs.columns());
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
