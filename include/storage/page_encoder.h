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
#include "common/coding.h"
#include "struct/ColumnValue.h"
#include "storage/page_encoder.h"
#include "storage/table_schema.h"

namespace LindormContest::storage {

static constexpr size_t PLAIN_PAGE_HEADER_SIZE = sizeof(UInt32);
static constexpr size_t PLAIN_PAGE_SIZE = 1024 * 1024; // default size: 1M

// static constexpr size_t BINARY_PLAIN_PAGE_HEADER_SIZE = sizeof(UInt32);
static constexpr size_t BINARY_PLAIN_PAGE_SIZE = 16 * 1024 * 1024; // default size: 16M

// PageBuilder is used to build page
// Page is a data management unit, including:
// 1. Data Page: store encoded and compressed data
// 2. BloomFilter Page: store bloom filter of data
// 3. Ordinal Index Page: store ordinal index of data
// 4. Short Key Index Page: store short key index of data
// 5. Bitmap Index Page: store bitmap index of data
class PageEncoder {
public:
    PageEncoder() = default;

    virtual ~PageEncoder() = default;

    virtual bool is_page_full() = 0;

    virtual void add(const uint8_t* data, size_t* count) = 0;

    virtual OwnedSlice finish() = 0;

    virtual void reset() = 0;

    virtual size_t count() const = 0;

    virtual size_t size() const = 0;

    virtual void get_first_value(void* value) const = 0;

    virtual void get_last_value(void* value) const = 0;
};

/**
 * page layout: [Slice0, Slice1, ..., SliceN, offset0, offset1, ..., offsetN, offsets_size]
 */
class BinaryPlainPageEncoder : public PageEncoder {
public:
    BinaryPlainPageEncoder() : _size(0) {
        _buffer.reserve(BINARY_PLAIN_PAGE_SIZE + 1024);
    }

    ~BinaryPlainPageEncoder() override = default;

    bool is_page_full() override {
        return _size >= BINARY_PLAIN_PAGE_SIZE;
    }

    // [Slice, Slice, Slice, ...]
    void add(const uint8_t* data, size_t* count) override {
        if (is_page_full()) {
            *count = 0;
            return;
        }
        size_t i = 0;

        while (!is_page_full() && i < *count) {
            const Slice* src = reinterpret_cast<const Slice*>(data);
            std::string str = src->to_string();
            _buffer.append(src->_data, src->_size);
            _offsets.push_back(_buffer.size());
            _size += (src->_size + sizeof(uint32_t));
            i++;
            data += sizeof(Slice);
        }

        *count = i;
    }

    OwnedSlice finish() override {
        for (uint32_t _offset : _offsets) {
            put_fixed32_le(&_buffer, _offset);
        }
        put_fixed32_le(&_buffer, _offsets.size());
        if (_offsets.size() > 0) {
            _copy_value_at(0, &_first_value);
            _copy_value_at(_offsets.size() - 1, &_last_value);
        }
        return _buffer;
    }

    void reset() override {
        _offsets.clear();
        _buffer.clear();
        _buffer.reserve(BINARY_PLAIN_PAGE_SIZE + 1024);
        _size = 0;
    }

    size_t count() const override {
        return _offsets.size();
    }

    size_t size() const override {
        return _size;
    }

    void get_first_value(void* value) const override {
        if (_offsets.size() == 0) {
            throw std::runtime_error("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_first_value);
    }

    void get_last_value(void* value) const override {
        if (_offsets.size() == 0) {
            throw std::runtime_error("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_last_value);
    }

    inline Slice operator[](size_t idx) const {
        assert(idx < _offsets.size());
        size_t value_size = idx == 0 ? _offsets[0] : _offsets[idx] - _offsets[idx - 1];
        const char* start_offset = idx == 0 ? _buffer.data() : _buffer.data() + _offsets[idx - 1];
        return Slice(start_offset, value_size);
    }

    inline Slice get(size_t idx) const {
        return (*this)[idx];
    }

private:
    void _copy_value_at(size_t idx, String* value) const {
        size_t value_size = idx == 0 ? _offsets[0] : _offsets[idx] - _offsets[idx - 1];
        const char* start_offset = idx == 0 ? _buffer.data() : _buffer.data() + _offsets[idx - 1];
        value->assign(start_offset, value_size);
    }

    size_t _size;
    String _buffer;
    std::vector<uint32_t> _offsets;
    String _first_value;
    String _last_value;
};

/**
 * page layout: [_count, item0, item1, ..., itemN]
 */
class PlainPageEncoder : public PageEncoder {
public:
    PlainPageEncoder(size_t type_size)
            : _type_size(type_size), _count(0) {
        // Reserve enough space for the page, plus a bit of slop since
        // we often overrun the page by a few values.
        _buffer.reserve(PLAIN_PAGE_SIZE + 1024);
        _buffer.resize(PLAIN_PAGE_HEADER_SIZE); // first item is _count
    }

    ~PlainPageEncoder() override = default;

    bool is_page_full() override {
        return _buffer.size() >= PLAIN_PAGE_SIZE;
    }

    void add(const UInt8* data, size_t* count) override {
        if (is_page_full()) {
            *count = 0;
            return;
        }

        size_t old_size = _buffer.size();
        size_t remaining = PLAIN_PAGE_SIZE - old_size;

        if ((*count * _type_size) <= remaining) {
            _buffer.resize(old_size + (*count) * _type_size);
            std::memcpy(&_buffer[old_size], data, (*count) * _type_size);
            _count += *count;
        } else {
            size_t write_count = remaining / _type_size + 1; // +1 means make data page overflow, and flush to disk
            _buffer.resize(old_size + write_count * _type_size);
            std::memcpy(&_buffer[old_size], data, write_count * _type_size);
            _count += write_count;
            *count = write_count;
        }
    }

    OwnedSlice finish() override {
        encode_fixed32_le((UInt8*)_buffer.data(), _count); // encode header, record total counts
        if (_count > 0) {
            _first_value.assign(&_buffer[PLAIN_PAGE_HEADER_SIZE], _type_size);
            _last_value.assign(&_buffer[PLAIN_PAGE_HEADER_SIZE + (_count - 1) * _type_size],
                               _type_size);
        }
        return _buffer;
    }

    void reset() override {
        _count = 0;
        _buffer.clear();
        _buffer.reserve(PLAIN_PAGE_SIZE + 1024);
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
        std::memcpy(value, _first_value.data(), _type_size);
    }

    void get_last_value(void* value) const override {
        if (_count == 0) {
            throw std::logic_error("page is empty");
        }
        std::memcpy(value, _last_value.data(), _type_size);
    }

private:
    size_t _type_size;
    String _buffer;
    size_t _count;
    String _first_value;
    String _last_value;
};

}