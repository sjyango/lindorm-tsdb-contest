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
#include "common/slice.h"
#include "io/io_utils.h"
#include "storage/segment_traits.h"

namespace LindormContest::storage {

class IndexPageEncoder {
public:
    IndexPageEncoder() = default;

    ~IndexPageEncoder() = default;

    void add(const Slice& key, const io::PagePointer& ptr) {
        put_length_prefixed_slice(&_buffer, key);
        ptr.encode_to(&_buffer);
        _count++;
    }

    size_t count() const {
        return _count;
    }

    void finish(OwnedSlice* body, IndexPageFooter* footer) {
        *body = OwnedSlice(_buffer);
        footer->_page_type = PageType::INDEX_PAGE;
        footer->_uncompressed_size = body->size();
        footer->_num_entries = _count;
    }

    uint64_t size() { return _buffer.size(); }

    // Return the key of the first entry in this index block.
    // The pointed-to data is only valid until the next call to this builder.
    // void get_first_key(Slice* key) const;

    void reset() {
        _buffer.clear();
        _count = 0;
    }

private:
    std::string _buffer;
    uint32_t _count = 0;
};

class IndexPageDecoder {
public:
    IndexPageDecoder() : _parsed(false) {}

    void load(const Slice& body, const IndexPageFooter& footer) {
        _footer = footer;
        size_t num_entries = _footer._num_entries;
        Slice input(body);

        for (int i = 0; i < num_entries; ++i) {
            Slice key;
            io::PagePointer value(0, 0);
            if (!get_length_prefixed_slice(&input, &key)) {
                ERR_LOG("Data corruption")
                throw std::runtime_error("Data corruption");
            }
            if (!value.decode_from(&input)) {
                ERR_LOG("Data corruption")
                throw std::runtime_error("Data corruption");
            }
            _keys.push_back(key);
            _values.push_back(value);
        }

        _parsed = true;
    }

    size_t count() const {
        assert(_parsed);
        return _footer._num_entries;
    }

    Slice get_key(uint32_t idx) const {
        assert(_parsed);
        assert(idx >= 0 && idx < _footer._num_entries);
        return _keys[idx];
    }

    io::PagePointer get_value(uint32_t idx) const {
        assert(_parsed);
        assert(idx >= 0 && idx < _footer._num_entries);
        return _values[idx];
    }

private:
    bool _parsed;

    IndexPageFooter _footer;
    std::vector<Slice> _keys;
    std::vector<io::PagePointer> _values;
};

class IndexPageIterator {
public:
    explicit IndexPageIterator(const IndexPageDecoder* decoder) : _decoder(decoder), _pos(0) {}

    // Find the largest index entry whose key is <= search_key.
    // Return `true` when such entry exists.
    // Return `false` when no such entry is found (all keys > search_key).
    // Return other error status otherwise.
    bool seek_at_or_before(const Slice& search_key) {
        int32_t left = 0;
        int32_t right = _decoder->count() - 1;

        while (left <= right) {
            int32_t mid = left + (right - left) / 2;
            int cmp = search_key.compare(_decoder->get_key(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                _pos = mid;
                return true;
            }
        }

        // no exact match, the insertion point is `left`
        if (left == 0) {
            // search key is smaller than all keys
            // throw std::runtime_error("given key is smaller than all keys in page");
            return false;
        }
        // index entry records the first key of the indexed page,
        // therefore the first page with keys >= searched key is the one before the insertion point
        _pos = left - 1;
        return true;
    }

    void seek_to_first() {
        _pos = 0;
    }

    // Move to the next index entry.
    // Return true on success, false when no more entries can be read.
    bool next() {
        _pos++;
        if (_pos >= _decoder->count()) {
            return false;
        }
        return true;
    }

    bool has_next() {
        return _pos < _decoder->count();
    }

    Slice current_key() const {
        return _decoder->get_key(_pos);
    }

    io::PagePointer current_page_pointer() const {
        return _decoder->get_value(_pos);
    }

private:
    const IndexPageDecoder* _decoder;
    size_t _pos;
};

}
