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
#include "page_encoder.h"
#include "struct/ColumnValue.h"
#include "table_schema.h"

namespace LindormContest::storage {

static const size_t PLAIN_PAGE_HEADER_SIZE = sizeof(UInt32);

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

    // Used by column writer to determine whether the current page is full.
    // Column writer depends on the result to decide whether to flush current page.
    virtual bool is_page_full() = 0;

    // Add a sequence of values to the page.
    // The number of values actually added will be returned through count, which may be less
    // than requested if the page is full.

    // check page if full before truly add, return ok when page is full so that column write
    // will switch to next page
    // vals size should be decided according to the page build type
    // TODO make sure vals is naturally-aligned to its type so that impls can use aligned load
    // instead of memcpy to copy values.
    virtual void add(const uint8_t* data, size_t* count) = 0;

    // Finish building the current page, return the encoded data.
    // This api should be followed by reset() before reusing the builder
    virtual OwnedSlice finish() = 0;

    // Get the dictionary page for dictionary encoding mode column.
    virtual void get_dictionary_page(OwnedSlice* dictionary_page) {
        throw std::runtime_error("get_dictionary_page not implemented");
    }

    // Reset the internal state of the page builder.
    //
    // Any data previously returned by finish may be invalidated by this call.
    virtual void reset() = 0;

    // Return the number of entries that have been added to the page.
    virtual size_t count() const = 0;

    // Return the total bytes of pageBuilder that have been added to the page.
    virtual UInt64 size() const = 0;

    // Return the first value in this page.
    // This method could only be called between finish() and reset().
    // void::NotFound if no values have been added.
    virtual void get_first_value(void* value) const = 0;

    // Return the last value in this page.
    // This method could only be called between finish() and reset().
    // void::NotFound if no values have been added.
    virtual void get_last_value(void* value) const = 0;
};

class BinaryPlainPageBuilder : public PageEncoder {
public:
    BinaryPlainPageBuilder(size_t data_page_size)
            : _data_page_size(data_page_size), _size_estimate(0) {
        reset();
    }

    bool is_page_full() override {
        return _size_estimate > _data_page_size;
    }

    void add(const uint8_t* data, size_t* count) override {
        assert(!_finished);
        assert(*count > 0);
        size_t i = 0;

        while (!is_page_full() && i < *count) {
            const Slice* src = reinterpret_cast<const Slice*>(data);
            size_t offset = _buffer.size();
            _offsets.push_back(offset);
            _buffer.append(src->_data, src->_size);
            _last_value_size = src->_size;
            _size_estimate += src->_size;
            _size_estimate += sizeof(uint32_t);
            i++;
            data += sizeof(Slice);
        }

        *count = i;
    }

    OwnedSlice finish() override {
        assert(!_finished);
        for (uint32_t _offset : _offsets) {
            put_fixed32_le(&_buffer, _offset);
        }
        put_fixed32_le(&_buffer, _offsets.size());
        if (_offsets.size() > 0) {
            _copy_value_at(0, &_first_value);
            _copy_value_at(_offsets.size() - 1, &_last_value);
        }
        _finished = true;
        return std::move(_buffer);
    }

    void reset() override {
        _offsets.clear();
        _buffer.clear();
        _buffer.reserve(_data_page_size);
        _size_estimate = sizeof(uint32_t);
        _finished = false;
        _last_value_size = 0;
    }

    size_t count() const override {
        return _offsets.size();
    }

    uint64_t size() const override {
        return _size_estimate;
    }

    void get_first_value(void* value) const override {
        assert(_finished);
        if (_offsets.size() == 0) {
            throw std::runtime_error("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_first_value);
    }

    void get_last_value(void* value) const override {
        assert(_finished);
        if (_offsets.size() == 0) {
            throw std::runtime_error("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_last_value);
    }

    inline Slice operator[](size_t idx) const {
        assert(!_finished);
        assert(idx < _offsets.size());
        size_t value_size = (idx < _offsets.size() - 1) ? _offsets[idx + 1] - _offsets[idx] : _last_value_size;
        return Slice(&_buffer[_offsets[idx]], value_size);
    }

    inline Slice get(size_t idx) const {
        return (*this)[idx];
    }

private:
    void _copy_value_at(size_t idx, String* value) const {
        size_t value_size = (idx < _offsets.size() - 1) ? _offsets[idx + 1] - _offsets[idx] : _last_value_size;
        value->assign(&_buffer[_offsets[idx]], value_size);
    }

    String _buffer;
    size_t _data_page_size;
    size_t _size_estimate;
    // Offsets of each entry, relative to the start of the page
    std::vector<uint32_t> _offsets;
    bool _finished;
    // size of last added value
    uint32_t _last_value_size = 0;
    String _first_value;
    String _last_value;
};

class PlainPageEncoder : public PageEncoder {
public:
    PlainPageEncoder(const TableColumn& column, size_t data_page_size)
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