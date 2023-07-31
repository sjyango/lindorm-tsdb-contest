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

#include "Root.h"
#include "table_schema.h"
#include "vec/columns/column_with_type_and_name.h"

namespace LindormContest::storage {

class ColumnDataConvertor {
public:
    ColumnDataConvertor() = default;

    ~ColumnDataConvertor() = default;

    virtual void set_source_column(const vectorized::ColumnWithTypeAndName& column, size_t row_pos, size_t num_rows) {
        assert(row_pos + num_rows <= column._column->size());
        _column = column;
        _row_pos = row_pos;
        _num_rows = num_rows;
    }

    virtual void convert() = 0;

    virtual const void* get_data() const = 0;

    virtual const void* get_data_at(size_t offset) const = 0;

protected:
    vectorized::ColumnWithTypeAndName _column;
    size_t _row_pos = 0;
    size_t _num_rows = 0;
};

template <typename T>
class NumberColumnDataConvertor : public ColumnDataConvertor {
public:
    NumberColumnDataConvertor() = default;

    ~NumberColumnDataConvertor() = default;

    const void* get_data() const override {
        return _data;
    }

    const void* get_data_at(size_t offset) const override {
        return _data + offset;
    }

    void convert() override {
        assert(_column._column);
        const vectorized::ColumnNumber<T>& column = reinterpret_cast<const vectorized::ColumnNumber<T>&>(*_column._column);
        _data = column.get_data().data() + _row_pos;
    }

private:
    const T* _data = nullptr;
};

class StringColumnDataConvertor : public ColumnDataConvertor {
public:
    StringColumnDataConvertor() = default;

    ~StringColumnDataConvertor() = default;

    const void* get_data() const override {
        return _data.data();
    }

    const void* get_data_at(size_t offset) const override {
        assert(offset < _data.size());
        return _data.data() + offset;
    }

    void convert() override {
        assert(_column._column);
        const vectorized::ColumnString& column = reinterpret_cast<const vectorized::ColumnString&>(*_column._column);
        const char* char_data = reinterpret_cast<const char*>(column.get_chars().data());

        for (size_t pos = _row_pos, i = 0; i < _num_rows; ++i) {
            const char* data = char_data + column.offset_at(pos + i);
            size_t size = column.size_at(pos + i);
            _data.emplace_back(const_cast<char*>(data), size);
        }

        assert(_data.size() == _num_rows);
    }

private:
    std::vector<Slice> _data;
};

class BlockDataConvertor {
public:
    BlockDataConvertor() = default;

    BlockDataConvertor(const TableSchema* schema) {
        assert(schema);
        for (const auto& col : schema->columns()) {
            _convertors.emplace_back(_create_column_data_convertor(col));
        }
    }

    BlockDataConvertor(const TableSchema* schema, const std::vector<UInt32>& col_ids) {
        assert(schema);
        for (const auto& id : col_ids) {
            const auto& col = schema->column(id);
            _convertors.emplace_back(_create_column_data_convertor(col));
        }
    }

    void set_source_content(const vectorized::Block* block, size_t row_pos, size_t num_rows) {
        assert(block && num_rows > 0 && row_pos + num_rows <= block->rows() && block->columns() == _convertors.size());
        size_t cid = 0;

        for (const auto& column : *block) {
            _convertors[cid]->set_source_column(column, row_pos, num_rows);
            ++cid;
        }
    }

    void set_source_content(const vectorized::Block* block, size_t row_pos, size_t num_rows, std::vector<uint32_t> cids) {
        assert(block && num_rows > 0 && row_pos + num_rows <= block->rows() && block->columns() <= _convertors.size());
        for (auto cid : cids) {
            _convertors[cid]->set_source_column(block->get_by_position(cid), row_pos, num_rows);
        }
    }

    ColumnDataConvertor* convert_column_data(size_t cid) {
        assert(cid < _convertors.size());
        _convertors[cid]->convert();
        return _convertors[cid].get();
    }

    void add_column_data_convertor(const TableColumn& column) {
        _convertors.emplace_back(_create_column_data_convertor(column));
    }

    bool empty() const { return _convertors.empty(); }

    void reserve(size_t size) { _convertors.reserve(size); }

    void reset() { _convertors.clear(); }

private:
    using ColumnDataConvertorSPtr = std::shared_ptr<ColumnDataConvertor>;

    ColumnDataConvertorSPtr _create_column_data_convertor(const TableColumn& column) {
        switch (column.get_column_type()) {
        case COLUMN_TYPE_INTEGER:
            return std::make_shared<NumberColumnDataConvertor<Int32>>();
        case COLUMN_TYPE_DOUBLE_FLOAT:
            return std::make_shared<NumberColumnDataConvertor<Float64>>();
        case COLUMN_TYPE_STRING:
            return std::make_shared<StringColumnDataConvertor>();
        case COLUMN_TYPE_TIMESTAMP:
            return std::make_shared<NumberColumnDataConvertor<Int64>>();
        default:
            return nullptr;
        }
    }

    std::vector<ColumnDataConvertorSPtr> _convertors;
};

}