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

    Block(std::initializer_list<ColumnWithTypeAndName> il) : data {il} {
        initialize_index_by_name();
    }

    Block(const ColumnsWithTypeAndName& columns) : data {columns} {
        initialize_index_by_name();
    }

    Block(Block&& block)
            : data(std::move(block.data)), index_by_name(std::move(block.index_by_name)) {}

    ~Block() = default;

    void initialize_index_by_name() {
        for (size_t i = 0, size = data.size(); i < size; ++i) {
            index_by_name[data[i]._column->get_name()] = i;
        }
    }

    void reserve(size_t count) {
        index_by_name.reserve(count);
        data.reserve(count);
    }

    size_t columns() const {
        return data.size();
    }

    SColumns get_columns() const {
        size_t num_columns = data.size();
        SColumns columns(num_columns);
        for (int i = 0; i < num_columns; ++i) {
            columns[i] = data[i]._column;
        }
        return columns;
    }

    size_t rows() const {
        for (const auto& elem : data) {
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
        for (const auto& elem : data) {
            res.push_back(elem._type);
        }
        return res;
    }

    Names get_names() const {
        Names res;
        res.reserve(columns());
        for (const auto& elem : data) {
            res.push_back(elem._name);
        }
        return res;
    }

    void clear() {
        data.clear();
        index_by_name.clear();
    }

    void insert(size_t position, const ColumnWithTypeAndName& elem);

    void insert(const ColumnWithTypeAndName& elem);

    void erase(size_t position);

    void erase(const std::set<size_t>& positions);

    void erase(const String& name);

    ColumnWithTypeAndName& get_by_position(size_t position) {
        assert(position < data.size());
        return data[position];
    }

    const ColumnWithTypeAndName& get_by_position(size_t position) const {
        return data[position];
    }

    SMutableColumns mutate_columns();

    Block copy_block() const;

    Block copy_block(const std::vector<int>& column_uids) const;

    Block clone_without_columns() const;

    Container::iterator begin() { return data.begin(); }

    Container::iterator end() { return data.end(); }

    Container::const_iterator begin() const { return data.begin(); }

    Container::const_iterator end() const { return data.end(); }

    Container::const_iterator cbegin() const { return data.cbegin(); }

    Container::const_iterator cend() const { return data.cend(); }

private:
    void erase_impl(size_t position);

    Container data;
    IndexByName index_by_name;
};

}