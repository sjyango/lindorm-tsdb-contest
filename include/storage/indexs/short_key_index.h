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
#include "common/status.h"
#include "common/coding.h"
#include "storage/segment_traits.h"

namespace LindormContest::storage {

class ShortKeyIndexBuilder {
public:
    ShortKeyIndexBuilder(uint32_t segment_id)
            : _segment_id(segment_id), _num_items(0) {}

    Status add_item(const Slice& key) {
        put_varint32(&_offset_buf, _key_buf.size());
        _key_buf.append((const char*) key._data, key._size);
        _num_items++;
        return Status::OK();
    }

    uint64_t size() { return _key_buf.size() + _offset_buf.size(); }

    Status finalize(uint32_t num_segment_rows, std::vector<Slice>* body, PageFooter* page_footer) {
        page_footer->_page_type = PageType::SHORT_KEY_PAGE;
        page_footer->_uncompressed_size = _key_buf.size() + _offset_buf.size();
        page_footer->_short_key_footer._num_items = _num_items;
        page_footer->_short_key_footer._key_bytes = _key_buf.size();
        page_footer->_short_key_footer._offset_bytes = _offset_buf.size();
        page_footer->_short_key_footer._segment_id = _segment_id;
        page_footer->_short_key_footer._num_segment_rows = num_segment_rows;
        body->emplace_back(_key_buf);
        body->emplace_back(_offset_buf);
        return Status::OK();
    }

private:
    uint32_t _segment_id;
    // uint32_t _num_rows_per_block;
    uint32_t _num_items;

    String _key_buf;
    String _offset_buf;
};

}