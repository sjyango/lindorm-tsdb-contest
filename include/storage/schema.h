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

#include <algorithm>
#include <unordered_set>

#include "Root.h"
#include "table_schema.h"

namespace LindormContest::storage {

// The class is used to represent row's format in memory. Each row contains
// multiple columns, some of which are key-columns (the rest are value-columns).
// NOTE: If both key-columns and value-columns exist, then the key-columns
// must be placed before value-columns.
class Schema;

using SchemaSPtr = std::shared_ptr<const Schema>;

class Schema {
public:
    Schema(TableSchemaSPtr table_schema) {
        _num_key_columns = table_schema->num_key_columns();
        for (const auto& col : table_schema->columns()) {
            _cols.emplace(col.get_uid(), col);
        }
    }

    Schema(const std::vector<TableColumn>& columns) {
        _num_key_columns = 2; // vin + timestamp
        for (const auto& col : columns) {
            _cols.emplace(col.get_uid(), col);
        }
    }

    Schema(const Schema& other) {
        _cols = other._cols;
        _num_key_columns = other._num_key_columns;
    }

    Schema& operator=(const Schema& other) {
        if (this != &other) {
            _cols = other._cols;
            _num_key_columns = other._num_key_columns;
        }
        return *this;
    }

    ~Schema() = default;

    size_t num_key_columns() const {
        return _num_key_columns;
    }

    size_t num_columns() const {
        return _cols.size();
    }

    std::vector<ColumnId> column_ids() const {
        std::vector<ColumnId> column_ids;
        for (const auto& col : _cols) {
            column_ids.push_back(col.first);
        }
        std::sort(column_ids.begin(), column_ids.end());
        return column_ids;
    }

    std::vector<ColumnId> unique_ids() const {
        return std::move(column_ids());
    }

    const TableColumn& column(ColumnId cid) const {
        auto it = _cols.find(cid);
        if (it == _cols.end()) {
            std::runtime_error("Column doesn't exist");
        }
        return it->second;
    }

    std::vector<TableColumn> columns() const {
        std::vector<TableColumn> columns;
        for (const auto& cid : column_ids()) {
            columns.emplace_back(_cols.at(cid));
        }
        return columns;
    }

    bool contains(ColumnId cid) const {
        return _cols.find(cid) != _cols.end();
    }

    vectorized::Block create_block() const {
        vectorized::Block block;
        for (const auto& col_id : column_ids()) {
            assert(_cols.find(col_id) != _cols.end());
            const TableColumn& table_column = _cols.at(col_id);
            vectorized::MutableColumnSPtr ptr = vectorized::ColumnFactory::instance().create_column(table_column.get_column_type(), table_column.get_name());
            block.insert({ptr, table_column.get_column_type(), table_column.get_name()});
        }
        return block;
    }

    // ColumnId column_id(ColumnId cid) const {
    //     auto it = _cols.find(cid);
    //     if (it == _cols.end()) {
    //         std::runtime_error("Column doesn't exist");
    //     }
    //     return it->second.get_uid();
    // }
    //
    // int32_t unique_id(ColumnId cid) const {
    //     auto it = _cols.find(cid);
    //     if (it == _cols.end()) {
    //         std::runtime_error("Column doesn't exist");
    //     }
    //     return it->second.get_uid();
    // }

private:
    size_t _num_key_columns;
    std::unordered_map<ColumnId, TableColumn> _cols;
};

}
