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
#include "vec/blocks/block.h"
#include "vec/columns/IColumn.h"
#include "struct/Row.h"

namespace LindormContest::vectorized {

class MutableBlock {
public:
    static std::unique_ptr<MutableBlock> build_mutable_block(Block* block) {
        if (block == nullptr) {
            return std::make_unique<MutableBlock>();
        } else {
            return std::make_unique<MutableBlock>(block);
        }
    }

    MutableBlock() = default;

    MutableBlock(Block* block)
            : _data {block->mutate_columns()}, _data_types(block->get_data_types()), _names(block->get_names()) {
        initialize_index_by_name();
    }

    MutableBlock(Block& block)
            : _data {block.mutate_columns()}, _data_types(block.get_data_types()), _names(block.get_names()) {
        initialize_index_by_name();
    }

    MutableBlock(Block&& block)
            : _data {block.mutate_columns()}, _data_types(block.get_data_types()), _names(block.get_names()) {
        initialize_index_by_name();
    }

    ~MutableBlock() {
        for (auto& col : _data) {
            col.reset();
        }
        _data.clear();
        _index_by_name.clear();
        _data_types.clear();
        _names.clear();
    }

    void initialize_index_by_name() {
        for (size_t i = 0, size = _data.size(); i < size; ++i) {
            _index_by_name[_data[i]->get_name()] = i;
        }
    }

    void reserve(size_t count) {
        for (auto& column : _data) {
            column->reserve(count);
        }
    }

    size_t rows() const {
        for (const auto& column : _data) {
            if (column) {
                return column->size();
            }
        }

        return 0;
    }

    ColumnType get_type_by_position(size_t position) const {
        return _data[position]->get_type();
    }

    MutableColumnSPtr get_column_by_position(size_t position) const {
        return _data[position];
    }

    MutableColumnSPtr get_column_by_name(const std::string& name) const {
        assert(_index_by_name.find(name) != _index_by_name.end());
        return _data[_index_by_name.at(name)];
    }

    size_t memory_usage() const {
        size_t mem_usage = 0;
        for (const auto& item : _data) {
            mem_usage += item->memory_usage();
        }
        return mem_usage;
    }

    size_t columns() const { return _data.size(); }

    bool empty() const { return rows() == 0; }

    void erase(const String& name);

    Block to_block(int start_column = 0);

    Block to_block(int start_column, int end_column);

    void add_row(const Block* block, size_t row);

    void add_row(const Row& row);

    void append_block(const Block* block, const size_t* row_begin, const size_t* row_end);

    void append_block(const Block* block, size_t row_begin, size_t length);

    int compare_one_column(size_t n, size_t m, size_t column_id) const;

    int compare_at(size_t n, size_t m, size_t num_columns, const MutableBlock& rhs) const;

    int compare_at(size_t n, size_t m, const std::vector<UInt32>* compare_columns,
                   const MutableBlock& rhs) const;

private:
    using Container = SMutableColumns;
    using DataTypes = std::vector<ColumnType>;
    using Names = std::vector<String>;
    using IndexByName = std::unordered_map<String, size_t>;

    Container _data;
    IndexByName _index_by_name;
    DataTypes _data_types;
    Names _names;
};

}