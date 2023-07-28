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
#include "common/slice.h"
#include "struct/ColumnValue.h"
#include "struct/Schema.h"
#include "vec/blocks/block.h"
#include "vec/columns/ColumnFactory.h"

namespace LindormContest::storage {

class TableColumn {
public:
    TableColumn(UInt32 uid, String col_name, ColumnType type, bool is_key = false)
            : _uid(uid), _col_name(col_name), _type(type), _is_key(is_key) {
        if (_type == COLUMN_TYPE_INTEGER) {
            _SIZE_OF_TYPE = sizeof(Int32);
        } else if (_type == COLUMN_TYPE_DOUBLE_FLOAT) {
            _SIZE_OF_TYPE = sizeof(Float64);
        } else if (_type == COLUMN_TYPE_STRING) {
            _SIZE_OF_TYPE = sizeof(Slice);
        } else if (_type == COLUMN_TYPE_TIMESTAMP) {
            _SIZE_OF_TYPE = sizeof(Int64);
        } else {
            _SIZE_OF_TYPE = 0;
        }
    }

    UInt32 get_uid() const { return _uid; }

    void set_uid(UInt32 uid) { _uid = uid; }

    String get_name() const { return _col_name; }

    void set_name(String col_name) { _col_name = col_name; }

    ColumnType get_type() const { return _type; }

    void set_type(ColumnType type) { _type = type; }

    UInt32 get_type_size() const { return _SIZE_OF_TYPE; }

    bool is_key() const { return _is_key; }

    friend bool operator==(const TableColumn& a, const TableColumn& b) {
        if (a._uid != b._uid) {
            return false;
        }
        if (a._col_name != b._col_name) {
            return false;
        }
        if (a._type != b._type)  {
            return false;
        }
        if (a._is_key != b._is_key) {
            return false;
        }
        return true;
    }

    friend bool operator!=(const TableColumn& a, const TableColumn& b) {
        return !(a == b);
    }

private:
    UInt32 _uid;
    String _col_name;
    ColumnType _type;
    bool _is_key = false;
    size_t _SIZE_OF_TYPE;
};


class TableSchema {
public:
    TableSchema(const Schema& schema) {
        size_t column_index = 0;
        _cols.emplace_back(column_index, "vin", ColumnType::COLUMN_TYPE_STRING, true);
        _field_name_to_index[_cols.back().get_name()] = column_index;
        _field_id_to_index[_cols.back().get_uid()] = column_index;
        column_index++;
        _cols.emplace_back(column_index, "timestamp", ColumnType::COLUMN_TYPE_TIMESTAMP, true);
        _field_name_to_index[_cols.back().get_name()] = column_index;
        _field_id_to_index[_cols.back().get_uid()] = column_index;
        column_index++;

        for (const auto& column : schema.columnTypeMap) {
            _cols.emplace_back(column_index, column.first, column.second, false);
            _field_name_to_index[_cols.back().get_name()] = column_index;
            _field_id_to_index[_cols.back().get_uid()] = column_index;
            column_index++;
        }

        _num_key_columns = 2; // vin + timestamp
        _num_columns = _num_key_columns + schema.columnTypeMap.size();
        assert(column_index == _num_columns);
    }

    size_t num_columns() const { return _num_columns; }

    size_t num_key_columns() const { return _num_key_columns; }

    size_t num_short_key_columns() const { return 1; }

    const TableColumn& column(size_t ordinal) const {
        assert(ordinal < _num_columns);
        return _cols[ordinal];
    }

    const TableColumn& column_by_name(const std::string& field_name) const {
        assert(_field_name_to_index.count(field_name) != 0);
        const auto& found = _field_name_to_index.find(field_name);
        return _cols[found->second];
    }

    bool have_column(const std::string& field_name) const {
        if (!_field_name_to_index.count(field_name)) {
            return false;
        }
        return true;
    }

    bool have_column(UInt32 uid) const {
        if (!_field_id_to_index.count(uid)) {
            return false;
        }
        return true;
    }

    const TableColumn& column_by_uid(UInt32 col_uid) const {
        assert(_field_id_to_index.count(col_uid) != 0);
        const auto& found = _field_id_to_index.find(col_uid);
        return _cols[found->second];
    }

    const std::vector<TableColumn>& columns() const {
        return _cols;
    }

    void clear_columns() {
        _field_name_to_index.clear();
        _field_id_to_index.clear();
        _num_columns = 0;
        _num_key_columns = 0;
        _cols.clear();
    }

    vectorized::Block create_block(const std::vector<UInt32>& return_columns) const {
        vectorized::Block block;
        for (int i = 0; i < return_columns.size(); ++i) {
            const auto& col = _cols[return_columns[i]];
            vectorized::MutableColumnSPtr ptr = vectorized::ColumnFactory::instance().create_column(col.get_type(), col.get_name());
            block.insert({ptr, col.get_type(), col.get_name()});
        }
        return block;
    }

    vectorized::Block create_block() const {
        vectorized::Block block;
        for (const auto& col : _cols) {
            vectorized::MutableColumnSPtr ptr = vectorized::ColumnFactory::instance().create_column(col.get_type(), col.get_name());
            block.insert({ptr, col.get_type(), col.get_name()});
        }
        return block;
    }

private:
    std::vector<TableColumn> _cols;
    size_t _num_columns = 0;
    size_t _num_key_columns = 0;
    std::unordered_map<std::string, int32_t> _field_name_to_index;
    std::unordered_map<int32_t, int32_t> _field_id_to_index;
};

}