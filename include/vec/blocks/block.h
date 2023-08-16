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
#include "utils.h"
#include "struct/Row.h"
#include "vec/columns/IColumn.h"
#include "vec/columns/column_with_type_and_name.h"

namespace LindormContest::vectorized {

class Block {
public:
    using DataTypes = std::vector<ColumnType>;
    using Names = std::vector<String>;
    using Container = ColumnsWithTypeAndName;
    using IndexByName = std::unordered_map<String, size_t>;

    Block() = default;

    Block(std::initializer_list<ColumnWithTypeAndName> il) : _data {il} {
        initialize_index_by_name();
    }

    Block(const ColumnsWithTypeAndName& columns) : _data {columns} {
        initialize_index_by_name();
    }

    Block(Block&& block)
            : _data(std::move(block._data)), _index_by_name(std::move(block._index_by_name)) {}

    ~Block() {
        for (auto& item : _data) {
            item._column.reset();
        }
        _data.clear();
        _index_by_name.clear();
    }

    bool operator==(const Block& rhs) const;

    bool operator!=(const Block& rhs) const;

    void initialize_index_by_name() {
        for (size_t i = 0, size = _data.size(); i < size; ++i) {
            _index_by_name.emplace(_data[i]._column->get_name(), i);
        }
        assert(_index_by_name.size() == _data.size());
    }

    void reserve(size_t count) {
        _index_by_name.reserve(count);
        _data.reserve(count);
    }

    size_t columns() const {
        return _data.size();
    }

    SColumns get_columns() const {
        size_t num_columns = _data.size();
        SColumns columns(num_columns);
        for (size_t i = 0; i < num_columns; ++i) {
            columns[i] = _data[i]._column;
        }
        return columns;
    }

    size_t rows() const {
        for (const auto& elem : _data) {
            if (elem._column) {
                return elem._column->size();
            }
        }
        return 0;
    }

    bool empty() const {
        return rows() == 0;
    }

    DataTypes get_data_types() const {
        DataTypes res;
        res.reserve(columns());
        for (const auto& elem : _data) {
            res.push_back(elem._type);
        }
        return res;
    }

    Names get_names() const {
        Names res;
        res.reserve(columns());
        for (const auto& elem : _data) {
            res.push_back(elem._name);
        }
        return res;
    }

    void clear() {
        _data.clear();
        _index_by_name.clear();
    }

    void insert(size_t position, const ColumnWithTypeAndName& elem);

    void insert(const ColumnWithTypeAndName& elem);

    void insert(ColumnWithTypeAndName&& elem);

    void erase(size_t position);

    void erase(const std::set<size_t>& positions);

    void erase(const String& name);

    ColumnWithTypeAndName& get_by_position(size_t position) {
        assert(position < _data.size());
        return _data[position];
    }

    const ColumnWithTypeAndName& get_by_position(size_t position) const {
        return _data[position];
    }

    void clear_column_data() {
        for (auto& elem : _data) {
            IColumn& column = const_cast<IColumn&>(*elem._column);
            column.clear();
        }
    }

    int compare_column_at(size_t n, size_t m, size_t col_idx, const Block& rhs) const {
        return get_by_position(col_idx)._column->compare_at(
                n, m, *(rhs.get_by_position(col_idx)._column));
    }

    size_t memory_usage() const {
        size_t mem_usage = 0;
        for (const auto& item : _data) {
            mem_usage += item._column->memory_usage();
        }
        return mem_usage;
    }

    // Row to_row(size_t num_row) const;

    // [start_row, end_row)
    std::vector<Row> to_rows(size_t start_row, size_t end_row) const;

    std::vector<Row> to_rows() const;

    SMutableColumns mutate_columns();

    Block copy_block() const;

    Block copy_block(const std::vector<int>& column_uids) const;

    Block clone_without_columns() const;

    int compare_at(size_t n, size_t m, size_t num_columns, const Block& rhs) const;

    Container::iterator begin() { return _data.begin(); }

    Container::iterator end() { return _data.end(); }

    Container::const_iterator begin() const { return _data.begin(); }

    Container::const_iterator end() const { return _data.end(); }

    Container::const_iterator cbegin() const { return _data.cbegin(); }

    Container::const_iterator cend() const { return _data.cend(); }

private:
    void erase_impl(size_t position);

    Container _data;
    IndexByName _index_by_name;
};

}