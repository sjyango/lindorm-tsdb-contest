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

    virtual std::pair<uint32_t, std::vector<Slice>> finish() = 0;

    virtual void reset() = 0;

    virtual size_t count() const = 0;

    virtual size_t size() const = 0;
};

/**
 * page layout: [Slice0, Slice1, ..., SliceN, offset0, offset1, ..., offsetN, offsets_size]
 */
class BinaryPlainPageEncoder : public PageEncoder {
public:
    BinaryPlainPageEncoder() : _size(0), _count(0) {}

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
            _page_slices.emplace_back(src->_data, src->_size);
            _size += (src->_size + sizeof(uint32_t));
            _count++;
            i++;
            data += sizeof(Slice);
        }

        *count = i;
    }

    std::pair<uint32_t, std::vector<Slice>> finish() override {
        return {_count, _page_slices};
    }

    void reset() override {
        _count = 0;
        _size = 0;
        _page_slices.clear();
    }

    size_t count() const override {
        return _count;
    }

    size_t size() const override {
        return _size;
    }

private:
    size_t _count;
    size_t _size;
    std::vector<Slice> _page_slices;
};

/**
 * page layout: [_count, item0, item1, ..., itemN]
 */
class PlainPageEncoder : public PageEncoder {
public:
    PlainPageEncoder(size_t type_size)
            : _type_size(type_size), _size(0), _count(0) {
        // Reserve enough space for the page, plus a bit of slop since
        // we often overrun the page by a few values.
        // _buffer.reserve(PLAIN_PAGE_SIZE + 1024);
        // _buffer.resize(PLAIN_PAGE_HEADER_SIZE); // first item is _count
    }

    ~PlainPageEncoder() override = default;

    bool is_page_full() override {
        return _size >= PLAIN_PAGE_SIZE;
    }

    void add(const UInt8* data, size_t* count) override {
        if (is_page_full()) {
            *count = 0;
            return;
        }
        size_t remaining = PLAIN_PAGE_SIZE - _size;
        if ((*count * _type_size) <= remaining) {
            _page_slices.emplace_back(data, (*count) * _type_size);
            _size += (*count) * _type_size;
            _count += *count;
        } else {
            size_t write_count = remaining / _type_size + 1; // +1 means make data page overflow, and flush to disk
            _page_slices.emplace_back(data, write_count * _type_size);
            _size += write_count * _type_size;
            _count += write_count;
            *count = write_count;
        }
    }

    std::pair<uint32_t, std::vector<Slice>> finish() override {
        return {_count, _page_slices};
    }

    void reset() override {
        _count = 0;
        _size = 0;
        _page_slices.clear();
    }

    size_t count() const override {
        return _count;
    }

    size_t size() const override {
        return _size;
    }

private:
    size_t _type_size;
    size_t _count;
    size_t _size;
    std::vector<Slice> _page_slices;
};

}