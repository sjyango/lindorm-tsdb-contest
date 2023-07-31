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

#include "Root.h"
#include "common/coding.h"
#include "storage/segment_traits.h"

namespace LindormContest::storage {

// Ordinal index is implemented by one IndexPage that stores
// the `first value ordinal` and `file pointer` for each data page.
class OrdinalIndexWriter {
public:
    OrdinalIndexWriter() = default;

    ~OrdinalIndexWriter() = default;

    void append_entry(ordinal_t ordinal, UInt32 index) {
        // put_fixed64_le(&_buffer, ordinal);
        // put_fixed32_le(&_buffer, index);
        put_varint64_varint32(&_buffer, ordinal, index);
        _num_items++;
    }

    size_t size() const {
        return _num_items;
    }

    void get_first_key(Slice* key) const {
        if (_num_items == 0) {
            throw std::logic_error("Index page is empty");
        }
        Slice input(_buffer);
        if (!get_length_prefixed_slice(&input, key)) {
            throw std::logic_error("Can't decode first key");
        }
    }

    std::shared_ptr<OrdinalIndexPage> finalize() {
        OwnedSlice data(std::move(_buffer));
        return std::make_shared<OrdinalIndexPage>(_buffer.size(), _num_items, std::move(data));
    }

private:
    String _buffer;
    size_t _num_items = 0;
};

class OrdinalIndexReader {
public:
    class OrdinalPageIndexIterator {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type = ordinal_t;
        using pointer = ordinal_t*;
        using reference = ordinal_t&;
        using difference_type = ordinal_t;

        OrdinalPageIndexIterator() : _reader(nullptr), _index(-1) {}

        OrdinalPageIndexIterator(const OrdinalIndexReader* reader, size_t index = 0)
                : _reader(reader), _index(index) {}

        OrdinalPageIndexIterator& operator-=(size_t step) {
            _index -= step;
            return *this;
        }

        OrdinalPageIndexIterator& operator+=(size_t step) {
            _index += step;
            return *this;
        }

        OrdinalPageIndexIterator& operator++() {
            ++_index;
            return *this;
        }

        OrdinalPageIndexIterator& operator--() {
            --_index;
            return *this;
        }

        bool operator!=(const OrdinalPageIndexIterator& other) {
            return _index != other._index || _reader != other._reader;
        }

        bool operator==(const OrdinalPageIndexIterator& other) {
            return _index == other._index && _reader == other._reader;
        }

        size_t operator-(const OrdinalPageIndexIterator& other) const {
            return _index - other._index;
        }

        inline void next() {
            assert(_index < _reader->_num_items);
            _index++;
        }

        inline bool valid() const {
            return _index >= 0 && _index < _reader->_num_items;
        }

        ordinal_t operator*() const {
            return _reader->get_first_ordinal(_index);
        }

        size_t index() const {
            return _index;
        }

    private:
        const OrdinalIndexReader* _reader;
        size_t _index;
    };

    OrdinalIndexReader() : _parsed(false), _num_items(0) {}

    void parse(const OrdinalIndexPage* page) {
        Slice data = page->_data.slice();
        _num_items = page->_num_items;
        _ordinals.resize(_num_items + 1);
        _indexs.resize(_num_items);

        for (int i = 0; i < _num_items; ++i) {
            ordinal_t ordinal;
            UInt32 index;
            if (!get_varint64(&data, &ordinal)) {
                throw std::logic_error("Fail to get varint `ordinal` from buffer");
            }
            if (!get_varint32(&data, &index)) {
                throw std::logic_error("Fail to get varint `index` from buffer");
            }
            _ordinals[i] = ordinal;
            _indexs[i] = index;
        }

        if (data._size != 0) {
            throw std::logic_error("Still has data after parse all key offset");
        }
        _parsed = true;
        // _ordinals[_num_items] = _num_values;
    }

    inline OrdinalPageIndexIterator begin() const {
        return OrdinalPageIndexIterator(this, 0);
    }

    inline OrdinalPageIndexIterator end() const {
        return OrdinalPageIndexIterator(this, _num_items);
    }

    ordinal_t get_first_ordinal(size_t index) const {
        return _ordinals[index];
    }

    ordinal_t get_last_ordinal(size_t index) const {
        return get_first_ordinal(index + 1) - 1;
    }

    OrdinalPageIndexIterator lower_bound(ordinal_t ordinal) const {
        assert(_parsed);
        return _seek<true>(ordinal);
    }

    // Return the iterator which locates the first item greater than the input key.
    OrdinalPageIndexIterator upper_bound(ordinal_t ordinal) const {
        assert(_parsed);
        return _seek<false>(ordinal);
    }

    bool is_parsed() const {
        return _parsed;
    }

    OrdinalPageIndexIterator seek_at_or_before(ordinal_t ordinal) {
        int32_t left = 0;
        int32_t right = _num_items - 1;
        while (left < right) {
            int32_t mid = (left + right + 1) / 2;
            if (_ordinals[mid] < ordinal) {
                left = mid;
            } else if (_ordinals[mid] > ordinal) {
                right = mid - 1;
            } else {
                left = mid;
                break;
            }
        }
        if (_ordinals[left] > ordinal) {
            return end();
        }
        return OrdinalPageIndexIterator(this, left);
    }

private:
    template <bool lower_bound>
    OrdinalPageIndexIterator _seek(ordinal_t ordinal) const {
        auto cmp = [](ordinal_t lhs, ordinal_t rhs) { return lhs < rhs; };
        if constexpr (lower_bound) {
            return std::lower_bound(begin(), end(), ordinal, cmp);
        } else {
            return std::upper_bound(begin(), end(), ordinal, cmp);
        }
    }

    bool _parsed;
    std::vector<ordinal_t> _ordinals; // _ordinals[i] = first ordinal of the i-th data page,
    std::vector<UInt32> _indexs;
    int _num_items;
    // ordinal_t _num_values; // equals to 1 + 'last ordinal of last data pages'
};

}