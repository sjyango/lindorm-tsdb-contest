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
#include <fstream>

#include "Root.h"
#include "common/data_type_factory.h"
#include "common/slice.h"
#include "struct/ColumnValue.h"
#include "struct/Schema.h"
#include "vec/blocks/block.h"
#include "vec/columns/ColumnFactory.h"

namespace LindormContest::storage {

class TableColumn {
public:
    TableColumn(uint32_t uid, String col_name, ColumnType type, bool is_key = false)
            : _type(DataTypeFactory::instance().get_column_data_type(type)),
              _uid(uid), _col_name(col_name), _is_key(is_key) {}

    uint32_t get_uid() const { return _uid; }

    void set_uid(uint32_t uid) { _uid = uid; }

    String get_name() const { return _col_name; }

    void set_name(String col_name) { _col_name = col_name; }

    const DataType* get_data_type() const { return _type; }

    ColumnType get_column_type() const { return _type->column_type(); }

    void set_type(const DataType* type) { _type = type; }

    uint32_t get_type_size() const { return _type->type_size(); }

    bool is_key() const { return _is_key; }

    friend bool operator==(const TableColumn& a, const TableColumn& b) {
        if (a._uid != b._uid) {
            return false;
        }
        if (a._col_name != b._col_name) {
            return false;
        }
        if (a._type->column_type() != b._type->column_type())  {
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
    uint32_t _uid;
    String _col_name;
    const DataType* _type;
    bool _is_key = false;
};

class TableSchema;

using TableSchemaSPtr = std::shared_ptr<const TableSchema>;

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

        for (uint32_t i = 0; i < _cols.size(); ++i) {
            assert(i == _cols[i].get_uid());
        }
    }

    size_t num_columns() const { return _num_columns; }

    size_t num_key_columns() const { return _num_key_columns; }

    size_t num_short_key_columns() const { return _num_key_columns; }

    const TableColumn& column(size_t ordinal) const {
        assert(ordinal < _num_columns);
        return _cols[ordinal];
    }

    const TableColumn& column_by_name(const std::string& field_name) const {
        assert(_field_name_to_index.count(field_name) != 0);
        const auto& it = _field_name_to_index.find(field_name);
        return _cols[it->second];
    }

    std::vector<TableColumn> column_by_names(const std::set<std::string>& field_names) const {
        if (field_names.empty()) {
            return _cols;
        }
        std::vector<TableColumn> columns;
        for (const auto& field_name : field_names) {
            assert(_field_name_to_index.count(field_name) != 0);
            const auto& it = _field_name_to_index.find(field_name);
            columns.emplace_back(_cols[it->second]);
        }
        return std::move(columns);
    }

    bool have_column(const std::string& field_name) const {
        if (!_field_name_to_index.count(field_name)) {
            return false;
        }
        return true;
    }

    bool have_column(uint32_t uid) const {
        if (!_field_id_to_index.count(uid)) {
            return false;
        }
        return true;
    }

    const TableColumn& column_by_uid(uint32_t col_uid) const {
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

    vectorized::Block create_block(const std::vector<uint32_t>& return_columns) const {
        vectorized::Block block;
        for (int i = 0; i < return_columns.size(); ++i) {
            const auto& col = _cols[return_columns[i]];
            vectorized::MutableColumnSPtr ptr = vectorized::ColumnFactory::instance().create_column(col.get_column_type(), col.get_name());
            block.insert({ptr, col.get_column_type(), col.get_name()});
        }
        return block;
    }

    vectorized::Block create_block() const {
        vectorized::Block block;
        for (const auto& col : _cols) {
            vectorized::MutableColumnSPtr ptr = vectorized::ColumnFactory::instance().create_column(col.get_column_type(), col.get_name());
            block.insert({ptr, col.get_column_type(), col.get_name()});
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

static std::string column_type_to_string(ColumnType type) {
    switch (type) {
    case COLUMN_TYPE_INTEGER:
        return "COLUMN_TYPE_INTEGER";
    case COLUMN_TYPE_TIMESTAMP:
        return "COLUMN_TYPE_TIMESTAMP";
    case COLUMN_TYPE_DOUBLE_FLOAT:
        return "COLUMN_TYPE_DOUBLE_FLOAT";
    case COLUMN_TYPE_STRING:
        return "COLUMN_TYPE_STRING";
    case COLUMN_TYPE_UNINITIALIZED:
        return "COLUMN_TYPE_UNINITIALIZED";
    }
    return "COLUMN_TYPE_UNINITIALIZED";
}

static ColumnType string_to_column_type(std::string s) {
    if (s == "COLUMN_TYPE_INTEGER") {
        return COLUMN_TYPE_INTEGER;
    }
    if (s == "COLUMN_TYPE_TIMESTAMP") {
        return COLUMN_TYPE_TIMESTAMP;
    }
    if (s == "COLUMN_TYPE_DOUBLE_FLOAT") {
        return COLUMN_TYPE_DOUBLE_FLOAT;
    }
    if (s == "COLUMN_TYPE_STRING") {
        return COLUMN_TYPE_STRING;
    }
    return COLUMN_TYPE_UNINITIALIZED;
}

static void save_schema_to_file(TableSchemaSPtr table_schema, std::string file_path) {
    std::ofstream output_file(file_path);

    for (const auto& column : table_schema->columns()) {
        output_file << column.get_name() << " " << column_type_to_string(column.get_column_type()) << std::endl;
    }

    output_file.close();
}

static TableSchemaSPtr load_schema_from_file(std::string file_path) {
    std::map<std::string, ColumnType> column_type_map;
    std::ifstream input_file(file_path);

    if (input_file.is_open()) {
        std::string column_name;
        std::string column_type_str;

        while (input_file >> column_name >> column_type_str) {
            ColumnType column_type = string_to_column_type(column_type_str);
            column_type_map[column_name] = column_type;
        }

        input_file.close();
    } else {
        throw std::runtime_error("Error opening schema file");
    }

    Schema schema;
    schema.columnTypeMap = std::move(column_type_map);
    return std::make_shared<TableSchema>(schema);
}

}