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

#include "vec/blocks/block.h"

namespace LindormContest::vectorized {

bool Block::operator==(const Block& rhs) const {
    if (columns() != rhs.columns()) {
        return false;
    }

    for (int i = 0; i < columns(); ++i) {
        if (get_by_position(i) != rhs.get_by_position(i)) {
            return false;
        }
    }

    return true;
}

bool Block::operator!=(const Block& rhs) const {
    return !(*this == rhs);
}

void Block::insert(size_t position, const ColumnWithTypeAndName& elem) {
    if (position > _data.size()) {
        std::cerr << "Position out of bound in Block::insert(), max position = " << _data.size() << std::endl;
    }

    for (auto& name_pos : _index_by_name) {
        if (name_pos.second >= position) {
            ++name_pos.second;
        }
    }

    _index_by_name.emplace(elem._name, position);
    _data.emplace(_data.begin() + position, elem);
}

void Block::insert(const ColumnWithTypeAndName& elem) {
    _index_by_name.emplace(elem._name, _data.size());
    _data.emplace_back(elem);
}

void Block::insert(ColumnWithTypeAndName&& elem) {
    _index_by_name.emplace(elem._name, _data.size());
    _data.emplace_back(elem);
}

void Block::erase(size_t position) {
    if (_data.empty()) {
        std::cerr << "Block is empty" << std::endl;
    }
    if (position >= _data.size()) {
        std::cerr << "Position out of bound in Block::erase(), max position = " << _data.size() - 1 << std::endl;
    }
    erase_impl(position);
}

void Block::erase(const std::set<size_t>& positions) {
    for (auto it = positions.rbegin(); it != positions.rend(); ++it) {
        erase(*it);
    }
}

void Block::erase(const String& name) {
    auto it = _index_by_name.find(name);
    if (it == _index_by_name.end()) {
        std::cerr << "No such name in `Block::erase()`: " << name << std::endl;
    }
    erase_impl(it->second);
}

void Block::erase_impl(size_t position) {
    _data.erase(_data.begin() + position);
    for (auto it = _index_by_name.begin(); it != _index_by_name.end();) {
        if (it->second == position) {
            _index_by_name.erase(it++);
        } else {
            if (it->second > position) {
                --it->second;
            }
            ++it;
        }
    }
}

SMutableColumns Block::mutate_columns() {
    size_t num_columns = _data.size();
    SMutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        if (_data[i]._column) {
            columns[i] = std::const_pointer_cast<IColumn>(_data[i]._column);
        } else {
            columns[i] = ColumnFactory::instance().create_column(_data[i]._type, _data[i]._name);
        }
    }
    return columns;
}

Block Block::copy_block() const {
    ColumnsWithTypeAndName columns_with_type_and_name;
    for (const auto& elem : _data) {
        columns_with_type_and_name.emplace_back(elem);
    }
    return {columns_with_type_and_name};
}

Block Block::copy_block(const std::vector<int>& column_uids) const {
    ColumnsWithTypeAndName columns_with_type_and_name;
    for (auto uid : column_uids) {
        assert(uid < _data.size());
        columns_with_type_and_name.emplace_back(_data[uid]);
    }
    return {columns_with_type_and_name};
}

Block Block::clone_without_columns() const {
    Block res;
    size_t num_columns = _data.size();
    for (int i = 0; i < num_columns; ++i) {
        res.insert({nullptr, _data[i]._type, _data[i]._name});
    }
    return res;
}

int Block::compare_at(size_t n, size_t m, size_t num_columns, const Block& rhs) const {
    assert(columns() >= num_columns);
    assert(rhs.columns() >= num_columns);
    assert(n <= rows());
    assert(m <= rhs.rows());

    for (size_t i = 0; i < num_columns; ++i) {
        assert(get_by_position(i)._type == rhs.get_by_position(i)._type);
        auto res = get_by_position(i)._column->compare_at(n, m, *(rhs.get_by_position(i)._column));
        if (res) {
            return res;
        }
    }
    return 0;
}

Row Block::to_row(size_t num_row) const {
    assert(num_row < rows());
    Row row;
    std::string vin_str = reinterpret_cast<const ColumnString&>(*get_by_position(0)._column).get(num_row);
    Vin vin;
    std::strncpy(vin.vin, vin_str.c_str(), 17);
    int64_t timestamp = reinterpret_cast<const ColumnInt64&>(*get_by_position(0)._column).get(num_row);
    std::map<std::string, ColumnValue> columns;

    for (const auto& col : *this) {
        switch (col._type) {
        case COLUMN_TYPE_INTEGER: {
            ColumnValue value = ColumnValue(reinterpret_cast<const ColumnInt32&>(*col._column).get(num_row));
            columns.emplace(col._name, std::move(value));
            break;
        }
        case COLUMN_TYPE_DOUBLE_FLOAT: {
            ColumnValue value = ColumnValue(reinterpret_cast<const ColumnFloat64&>(*col._column).get(num_row));
            columns.emplace(col._name, std::move(value));
            break;
        }
        case COLUMN_TYPE_STRING: {
            ColumnValue value = ColumnValue(reinterpret_cast<const ColumnString&>(*col._column).get(num_row));
            columns.emplace(col._name, std::move(value));
            break;
        }
        default: {
            throw std::runtime_error("unknown column type");
        }
        }
    }

    row.vin = vin;
    row.timestamp = timestamp;
    row.columns = std::move(columns);
    return std::move(row);
}

// [start_row, end_row)
std::vector<Row> Block::to_rows(size_t start_row, size_t end_row) const {
    assert(start_row >= 0 && end_row <= rows() && start_row < end_row);
    std::vector<Row> rows;

    for (int i = start_row; i < end_row; ++i) {
        Row row;
        std::string vin_str = reinterpret_cast<const ColumnString&>(*get_by_position(0)._column).get(i);
        Vin vin;
        std::strncpy(vin.vin, vin_str.c_str(), 17);
        int64_t timestamp = reinterpret_cast<const ColumnInt64&>(*get_by_position(1)._column).get(i);
        std::map<std::string, ColumnValue> columns;

        for (const auto& col : *this) {
            if (col._name == "vin" || col._name == "timestamp") {
                continue;
            }
            switch (col._type) {
            case COLUMN_TYPE_INTEGER: {
                ColumnValue value = ColumnValue(reinterpret_cast<const ColumnInt32&>(*col._column).get(i));
                columns.emplace(col._name, std::move(value));
                break;
            }
            case COLUMN_TYPE_DOUBLE_FLOAT: {
                ColumnValue value = ColumnValue(reinterpret_cast<const ColumnFloat64&>(*col._column).get(i));
                columns.emplace(col._name, std::move(value));
                break;
            }
            case COLUMN_TYPE_STRING: {
                ColumnValue value = ColumnValue(reinterpret_cast<const ColumnString&>(*col._column).get(i));
                columns.emplace(col._name, std::move(value));
                break;
            }
            default: {
                throw std::runtime_error("unknown column type");
            }
            }
        }

        row.vin = vin;
        row.timestamp = timestamp;
        row.columns = std::move(columns);
        rows.emplace_back(std::move(row));
    }

    assert(rows.size() == (end_row - start_row));
    return std::move(rows);
}

std::vector<Row> Block::to_rows() const {
    return std::move(to_rows(0, rows()));
}

}