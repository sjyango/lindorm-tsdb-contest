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
    ShortKeyIndexWriter(UInt32 segment_id)
            : _segment_id(segment_id), _num_items(0) {}

    void add_item(const String& key) {
        put_varint32(&_offset_buffer, _key_buffer.size());
        _key_buffer.append(key.c_str(), key.size());
        _num_items++;
    }

    size_t size() {
        return _key_buffer.size() + _offset_buffer.size();
    }

    std::shared_ptr<ShortKeyIndexPage> finalize(UInt32 num_segment_rows) {
        size_t key_size = _key_buffer.size();
        size_t offset_size = _offset_buffer.size();
        _key_buffer.append(_offset_buffer);
        OwnedSlice data(std::move(_key_buffer));
        return std::make_shared<ShortKeyIndexPage>(
                        key_size + offset_size,
                        _num_items,
                        key_size,
                        offset_size,
                        _segment_id,
                        num_segment_rows,
                        std::move(data)
                );
    }

private:
    UInt32 _segment_id;
    size_t _num_items;
    String _key_buffer;
    String _offset_buffer;
};

// Used to decode short key to header and encoded index data.
// Usage:
//      ShortKeyIndexDecoder decoder;
//      decoder.parse(body, footer);
//      auto iter = decoder.lower_bound(key);
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

    void parse(const ShortKeyIndexPage* page) {
        _page = page;
        Slice data = page->_data.slice();
        assert(data.size() == (page->_key_bytes + page->_offset_bytes));
        // set index buffer
        _short_key_data = Slice(data._data, page->_key_bytes);
        // parse offset information
        Slice offset_slice(data._data + page->_key_bytes, page->_offset_bytes);
        // +1 for record total length
        _offsets.resize(page->_num_items + 1);
        
        for (UInt32 i = 0; i < page->_num_items; ++i) {
            UInt32 offset = 0;
            if (!get_varint32(&offset_slice, &offset)) {
                throw std::logic_error("Fail to get varint from index offset buffer");
            }
            assert(offset <= page->_key_bytes);
            _offsets[i] = offset;
        }

        _offsets[page->_num_items] = page->_key_bytes;
        if (offset_slice._size != 0) {
            throw std::logic_error("Still has data after parse all key offset");
        }
        _parsed = true;
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
        return _page->_num_items;
    }

    Slice key(size_t index) const {
        assert(_parsed);
        assert(index >= 0 && index < num_items());
        return {_short_key_data._data + _offsets[index], _offsets[index + 1] - _offsets[index]};
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
    const ShortKeyIndexPage* _page;
    std::vector<UInt32> _offsets;
    Slice _short_key_data;
};

}