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

class ShortKeyIndexWriter {
public:
    ShortKeyIndexWriter() : _num_items(0) {}

    void add_item(const String& key) {
        // put_varint32(&_offset_buffer, _key_buffer.size());
        put_fixed32_le(&_offset_buffer, _key_buffer.size());
        _key_buffer.append(key.c_str(), key.size());
        _num_items++;
    }

    size_t size() {
        return _key_buffer.size() + _offset_buffer.size();
    }

    void finalize(uint32_t num_segment_rows, OwnedSlice* body, ShortKeyIndexFooter* footer) {
        footer->_page_type = PageType::SHORT_KEY_PAGE;
        footer->_uncompressed_size = _key_buffer.size() + _offset_buffer.size();
        footer->_num_items = _num_items;
        footer->_key_bytes = _key_buffer.size();
        footer->_offset_bytes = _offset_buffer.size();
        footer->_num_segment_rows = num_segment_rows;
        body->_owned_data = std::make_unique<uint8_t[]>(_key_buffer.size() + _offset_buffer.size());
        body->_size = _key_buffer.size() + _offset_buffer.size();
        std::memcpy(body->_owned_data.get(), _key_buffer.c_str(), _key_buffer.size());
        std::memcpy(body->_owned_data.get() + _key_buffer.size(), _offset_buffer.c_str(), _offset_buffer.size());
    }

private:
    size_t _num_items;
    String _key_buffer;
    String _offset_buffer;
};

class ShortKeyIndexReader {
public:
    // An Iterator to iterate one short key index.
    // Client can use this class to iterator all items in this index.
    class ShortKeyIndexIterator {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type = Slice;
        using pointer = Slice*;
        using reference = Slice&;
        using difference_type = size_t;

        ShortKeyIndexIterator(const ShortKeyIndexReader* reader, size_t index = 0)
                : _reader(reader), _index(index) {}

        ShortKeyIndexIterator& operator-=(size_t step) {
            _index -= step;
            return *this;
        }

        ShortKeyIndexIterator& operator+=(size_t step) {
            _index += step;
            return *this;
        }

        ShortKeyIndexIterator& operator++() {
            _index++;
            return *this;
        }

        ShortKeyIndexIterator& operator--() {
            _index--;
            return *this;
        }

        bool operator!=(const ShortKeyIndexIterator& other) {
            return _index != other._index || _reader != other._reader;
        }

        bool operator==(const ShortKeyIndexIterator& other) {
            return _index == other._index && _reader == other._reader;
        }

        size_t operator-(const ShortKeyIndexIterator& other) const {
            return _index - other._index;
        }

        inline bool valid() const {
            return _index >= 0 && _index < _reader->num_items();
        }

        Slice operator*() const {
            return _reader->key(_index);
        }

        // return current block index[size=1024]
        size_t index() const {
            return _index;
        }

    private:
        const ShortKeyIndexReader* _reader;
        size_t _index;
    };

    ShortKeyIndexReader() : _parsed(false) {}

    void load(const Slice& body, const ShortKeyIndexFooter& footer) {
        _footer = footer;
        assert(body._size == (_footer._key_bytes + _footer._offset_bytes));
        _short_key_data = OwnedSlice(body._data, _footer._key_bytes);
        Slice offset_slice(body._data + _footer._key_bytes, _footer._offset_bytes);
        _offsets.resize(_footer._num_items + 1);
        // +1 for record total length

        for (uint32_t i = 0; i < _footer._num_items; ++i) {
            uint32_t offset = decode_fixed32_le(reinterpret_cast<const uint8_t*>(offset_slice._data));
            offset_slice._data += sizeof(uint32_t);
            offset_slice._size -= sizeof(uint32_t);
            assert(offset <= _footer._key_bytes);
            _offsets[i] = offset;
        }

        _offsets[_footer._num_items] = _footer._key_bytes;
        if (offset_slice._size != 0) {
            ERR_LOG("Still has data after parse all key offset")
            throw std::logic_error("Still has data after parse all key offset");
        }
        _parsed = true;
        // INFO_LOG("load short key index success, index data size is %zu, num items is %u", _short_key_data.size(), _footer._num_items)
    }

    inline ShortKeyIndexIterator begin() const {
        assert(_parsed);
        return {this, 0};
    }

    inline ShortKeyIndexIterator end() const {
        assert(_parsed);
        return {this, num_items()};
    }

    // Return an iterator which locates at the first item who is `equal with or greater than` the given key.
    // NOTE: If one key is the prefix of other key, this function thinks
    // that longer key is greater than the shorter key.
    // return first key >= parameter key
    ShortKeyIndexIterator lower_bound(const Slice& key) const {
        assert(_parsed);
        return _seek<true>(key);
    }

    // Return the iterator which locates the first item `greater than` the input key.
    // return first key > parameter key
    ShortKeyIndexIterator upper_bound(const Slice& key) const {
        assert(_parsed);
        return _seek<false>(key);
    }

    UInt32 num_items() const {
        assert(_parsed);
        return _footer._num_items;
    }

    Slice key(size_t index) const {
        assert(_parsed);
        assert(index >= 0 && index < num_items());
        return {_short_key_data.data() + _offsets[index], _offsets[index + 1] - _offsets[index]};
    }

private:
    template <bool lower_bound>
    ShortKeyIndexIterator _seek(const Slice& key) const {
        auto cmp = [](const Slice& lhs, const Slice& rhs) {
            return lhs.compare(rhs) < 0;
        };
        if constexpr (lower_bound) {
            return std::lower_bound(begin(), end(), key, cmp);
        } else {
            return std::upper_bound(begin(), end(), key, cmp);
        }
    }

    bool _parsed;
    ShortKeyIndexFooter _footer;
    std::vector<uint32_t> _offsets;
    OwnedSlice _short_key_data;
};

}