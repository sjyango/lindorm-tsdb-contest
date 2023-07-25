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
#include "common/status.h"
#include "page_builder.h"
#include "segment_traits.h"
#include "struct/ColumnValue.h"

namespace LindormContest::storage {

// IndexPage is the building block for IndexedColumn's ordinal index and value index.
// It is used to guide searching for a particular key to the data page containing it.
// All keys are treated as binary string and compared with memcpy. Keys of other data type are encoded first by
// KeyCoder, e.g., ordinal index's original key type is uint64_t but is encoded to binary string.
class IndexPageBuilder {
public:
    explicit IndexPageBuilder(size_t index_page_size, bool is_leaf)
            : _index_page_size(index_page_size), _is_leaf(is_leaf) {}

    void add(const Slice& key, const PagePointer& page_pointer) {
        assert(!_finished);
        put_length_prefixed_slice(&_buffer, key);
        page_pointer.encode_to(&_buffer);
        _count++;
    }

    bool is_full() const { return _buffer.size() > _index_page_size; }

    size_t count() const { return _count; }

    OwnedSlice finish(PageFooter* footer) {
        footer->_page_type = PageType::INDEX_PAGE;
        footer->_uncompressed_size = _buffer.size();
        footer->_index_page_footer._num_entries = _count;
        footer->_index_page_footer._type = _is_leaf ? IndexPageFooter::IndexPageType::LEAF : IndexPageFooter::IndexPageType::INTERNAL;
        _finished = true;
        return std::move(_buffer);
    }

    uint64_t size() { return _buffer.size(); }

    // Return the key of the first entry in this index block.
    // The pointed-to data is only valid until the next call to this builder.
    Status get_first_key(Slice* key) const {
        if (_count == 0) {
            return Status::NotFound("index page is empty");
        }
        Slice input(_buffer);
        if (get_length_prefixed_slice(&input, key)) {
            return Status::OK();
        } else {
            return Status::Corruption("can't decode first key");
        }
    }

    void reset() {
        _finished = false;
        _buffer.clear();
        _count = 0;
    }

private:
    const size_t _index_page_size;
    const bool _is_leaf;
    bool _finished = false;
    String _buffer;
    uint32_t _count = 0;
};

}