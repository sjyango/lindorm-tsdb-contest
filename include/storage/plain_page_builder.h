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
#include "page_builder.h"
#include "common/coding.h"
#include "struct/ColumnValue.h"
#include "table_schema.h"

namespace LindormContest::storage {

static const size_t PLAIN_PAGE_HEADER_SIZE = sizeof(UInt32);

class PlainPageBuilder : public PageBuilder {
public:
    PlainPageBuilder(const TableColumn& column, size_t data_page_size)
            : _column(column), _data_page_size(data_page_size), _count(0) {
        // Reserve enough space for the page, plus a bit of slop since
        // we often overrun the page by a few values.
        _buffer.reserve(data_page_size + 1024);
        _buffer.clear();
        _buffer.resize(PLAIN_PAGE_HEADER_SIZE);
    }

    bool is_page_full() override {
        return _buffer.size() > _data_page_size;
    }

    void add(const UInt8* data, size_t* count) override {
        if (is_page_full()) {
            *count = 0;
            return;
        }
        size_t old_size = _buffer.size();
        _buffer.resize(old_size + (*count) * _column.get_type_size());
        std::memcpy(&_buffer[old_size], data, (*count) * _column.get_type_size());
        _count += *count;
    }

    OwnedSlice finish() override {
        encode_fixed32_le((UInt8*) &_buffer[0], _count); // encode header, record total counts
        if (_count > 0) {
            _first_value.assign(&_buffer[PLAIN_PAGE_HEADER_SIZE], _column.get_type_size());
            _last_value.assign(&_buffer[PLAIN_PAGE_HEADER_SIZE + (_count - 1) * _column.get_type_size()],
                                    _column.get_type_size());
        }
        return std::move(_buffer);
    }

    void reset() override {
        _count = 0;
        _buffer = std::string();
        _buffer.reserve(_data_page_size + 1024);
        _buffer.resize(PLAIN_PAGE_HEADER_SIZE);
    }

    size_t count() const override {
        return _count;
    }

    size_t size() const override {
        return _buffer.size();
    }

    void get_first_value(void* value) const override {
        if (_count == 0) {
            throw std::logic_error("page is empty");
        }
        std::memcpy(value, _first_value.data(), _column.get_type_size());
    }

    void get_last_value(void* value) const override {
        if (_count == 0) {
            throw std::logic_error("page is empty");
        }
        std::memcpy(value, _last_value.data(), _column.get_type_size());
    }

private:
    const TableColumn& _column;
    String _buffer;
    size_t _data_page_size;
    size_t _count;
    String _first_value;
    String _last_value;
};

}