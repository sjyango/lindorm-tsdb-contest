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
#include "storage/segment_traits.h"

namespace LindormContest::storage {

// PageDecoder is used to decode page
class PageDecoder {
public:
    PageDecoder() = default;

    virtual ~PageDecoder() = default;

    virtual void init(Slice data) = 0;

    // Seek the decoder to the given positional index of the page.
    // For example, seek_to_position_in_page(0) seeks to the first
    // stored entry.
    //
    // It is an error to call this with a value larger than Count().
    // Doing so has undefined results.
    virtual void seek_to_position_in_page(size_t pos) = 0;

    // Seek the decoder to the given value in the seek_to_position_in_pagepage, or the
    // lowest value which is greater than the given value.
    //
    // If the decoder was able to locate an exact match, then
    // sets *exact_match to true. Otherwise sets *exact_match to
    // false, to indicate that the seeked value is _after_ the
    // requested value.
    //
    // If the given value is less than the lowest value in the page,
    // seeks to the start of the page. If it is higher than the highest
    // value in the page, then returns void::NotFound
    //
    // This will only return valid results when the data page
    // consists of values in sorted order.
    virtual void seek_at_or_after_value(const void* value, bool* exact_match) {
        throw std::runtime_error("seek_at_or_after_value not implemented");
    }

    // Seek the decoder forward by a given number of rows, or to the end
    // of the page. This is primarily used to skip over data.
    //
    // Return the step skipped.
    virtual size_t seek_forward(size_t n) {
        size_t step = std::min(n, count() - current_index());
        assert(step > 0);
        seek_to_position_in_page(current_index() + step);
        return step;
    }

    virtual void next_batch(size_t* n, vectorized::MutableColumnSPtr& dst) = 0;

    virtual void read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal, size_t* n,
                                  vectorized::MutableColumnSPtr& dst) {
        throw std::runtime_error("read_by_rowids not implemented");
    }

    // Same as `next_batch` except for not moving forward the cursor.
    // When read array's ordinals in `ArrayFileColumnIterator`, we want to read one extra ordinal
    // but do not want to move forward the cursor.
    virtual void peek_next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
        throw std::runtime_error("peek_next_batch not implemented");
    }

    // Return the number of elements in this page.
    virtual size_t count() const = 0;

    // Return the position within the page of the currently seeked
    // entry (ie the entry that will next be returned by next_vector())
    virtual size_t current_index() const = 0;

    bool has_remaining() const {
        return current_index() < count();
    }
};

/**
 * page layout: [Slice0, Slice1, ..., SliceN, offset0, offset1, ..., offsetN, offsets_size]
 */
class BinaryPlainPageDecoder : public PageDecoder {
public:
    BinaryPlainPageDecoder()
            : _num_elems(0), _offsets_pos(0), _cur_idx(0) {}

    void init(Slice data) override {
        _data = data;
        assert(_data._size >= 0);
        _num_elems = decode_fixed32_le(
                reinterpret_cast<const uint8_t*>(_data._data + (_data._size - sizeof(uint32_t))));
        _offsets_pos = _data._size - (_num_elems + 1) * sizeof(uint32_t);
    }

    void seek_to_position_in_page(size_t pos) override {
        assert(pos < _num_elems);
        _cur_idx = pos;
    }

    void next_batch(size_t* n, vectorized::MutableColumnSPtr& dst) override {
        if (*n == 0 || _cur_idx >= _num_elems) {
            *n = 0;
            return;
        }
        const size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elems - _cur_idx));
        uint32_t start_offset = _cur_idx == 0 ? 0 : _get_offset(_cur_idx);
        uint32_t offsets[max_fetch];

        for (size_t i = 0; i < max_fetch; ++i) {
            offsets[i] = _get_offset(++_cur_idx) - start_offset;
        }

        dst->insert_binary_data(_data._data + start_offset, offsets, max_fetch);
        *n = max_fetch;
    }

    size_t count() const override {
        return _num_elems;
    }

    size_t current_index() const override {
        return _cur_idx;
    }

    // void read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal, size_t* n,
    //                       vectorized::MutableColumnSPtr& dst) override {
    //     assert(_parsed);
    //     if (*n == 0) {
    //         *n = 0;
    //         return;
    //     }
    //     auto total = *n;
    //     size_t read_count = 0;
    //     uint32_t len_array[total];
    //     uint32_t start_offset_array[total];
    //     for (size_t i = 0; i < total; ++i) {
    //         ordinal_t ord = rowids[i] - page_first_ordinal;
    //         if (ord >= _num_elems) {
    //             break;
    //         }
    //         const uint32_t start_offset = _offset(ord);
    //         start_offset_array[read_count] = start_offset;
    //         len_array[read_count] = _offset(ord + 1) - start_offset;
    //         read_count++;
    //     }
    //
    //     if (read_count > 0) {
    //         dst->insert_binary_data(_data._data, len_array, start_offset_array, read_count);
    //     }
    //
    //     *n = read_count;
    // }

    // Slice string_at_index(size_t idx) const {
    //     const uint32_t start_offset = offset(idx);
    //     uint32_t len = offset(idx + 1) - start_offset;
    //     return Slice(&_data[start_offset], len);
    // }

private:
    uint32_t _get_offset(size_t idx) const {
        if (idx == 0) {
            return 0;
        }
        const uint8_t* p = reinterpret_cast<const uint8_t*>(_data._data + _offsets_pos + (idx - 1) * sizeof(uint32_t));
        return decode_fixed32_le(p);
    }

    Slice _data;
    uint32_t _num_elems;
    uint32_t _offsets_pos;
    uint32_t _cur_idx;
};

class PlainPageDecoder : public PageDecoder {
public:
    PlainPageDecoder(const DataType* type)
            : _type(type), _num_elems(0), _cur_idx(0) {}

    ~PlainPageDecoder() override = default;

    void init(Slice data) override {
        _data = data;
        assert(_data._size >= PLAIN_PAGE_HEADER_SIZE);
        _num_elems = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data._data));
        assert(_data._size == (PLAIN_PAGE_HEADER_SIZE + _num_elems * _type_size()));
    }

    void seek_to_position_in_page(size_t pos) override {
        assert(_num_elems > 0 && pos < _num_elems);
        _cur_idx = pos;
    }

    void seek_at_or_after_value(const void* value, bool* exact_match) override {
        assert(_num_elems > 0);
        size_t left = 0;
        size_t right = _num_elems;
        const void* mid_value;

        // find the first value >= target. after loop,
        // - left == index of first value >= target when found
        // - left == _num_elems when not found (all values < target)
        while (left < right) {
            size_t mid = left + (right - left) / 2;
            mid_value = _data._data + (PLAIN_PAGE_HEADER_SIZE + mid * _type_size());
            if (_type->cmp(mid_value, value) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if (left >= _num_elems) {
            throw std::logic_error("all value small than the value");
        }
        const void* find_value = _data._data + PLAIN_PAGE_HEADER_SIZE + left * _type_size();
        if (_type->cmp(find_value, value) == 0) {
            *exact_match = true;
        } else {
            *exact_match = false;
        }
        _cur_idx = left;
    }

    void next_batch(size_t* n, vectorized::MutableColumnSPtr& dst) override {
        if (*n == 0 || _cur_idx >= _num_elems) {
            *n = 0;
            return;
        }
        const size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elems - _cur_idx));
        const uint8_t* start_offset =
                reinterpret_cast<const uint8_t*>(_data._data + PLAIN_PAGE_HEADER_SIZE + (_cur_idx * _type_size()));
        _cur_idx += max_fetch;
        assert(_type->column_type() == COLUMN_TYPE_INTEGER
               || _type->column_type() == COLUMN_TYPE_TIMESTAMP
               || _type->column_type() == COLUMN_TYPE_DOUBLE_FLOAT);
        dst->insert_many_data(start_offset, max_fetch);
        *n = max_fetch;
    }

    size_t count() const override {
        return _num_elems;
    }

    size_t current_index() const override {
        return _cur_idx;
    }

private:
    size_t _type_size() const {
        return _type->type_size();
    }

    Slice _data;
    const DataType* _type;
    uint32_t _num_elems;
    uint32_t _cur_idx;
};

}