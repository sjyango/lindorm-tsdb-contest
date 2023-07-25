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

namespace LindormContest::storage {

enum class EncodingType {
    UNKNOWN_ENCODING,
    PLAIN_ENCODING
};

enum class CompressionType {
    UNKNOWN_ENCODING,
    NO_COMPRESSION
};

enum class PageType {
    UNKNOWN_PAGE,
    DATA_PAGE,
    INDEX_PAGE,
    SHORT_KEY_PAGE
};

struct PagePointer {
    UInt64 _offset;
    UInt32 _size;

    PagePointer() : _offset(0), _size(0) {}

    PagePointer(UInt64 offset, UInt32 size) : _offset(offset), _size(size) {}

    void reset() {
        _offset = 0;
        _size = 0;
    }

    const UInt8* decode_from(const UInt8* data, const UInt8* limit) {
        data = decode_varint64_ptr(data, limit, &_offset);
        if (data == nullptr) {
            return nullptr;
        }
        return decode_varint32_ptr(data, limit, &_size);
    }

    bool decode_from(Slice* input) {
        bool result = get_varint64(input, &_offset);
        if (!result) {
            return false;
        }
        return get_varint32(input, &_size);
    }

    void encode_to(String* dst) const {
        put_varint64_varint32(dst, _offset, _size);
    }

    bool operator==(const PagePointer& other) const {
        return _offset == other._offset && _size == other._size;
    }

    bool operator!=(const PagePointer& other) const { return !(*this == other); }
};

struct DataPageFooter {
    UInt64 _first_ordinal;
    UInt64 _num_values;
};

struct IndexPageFooter {
    enum class IndexPageType {
        UNKNOWN_INDEX_PAGE_TYPE,
        LEAF,
        INTERNAL
    };

    UInt32 _num_entries;
    IndexPageType _type;
};

struct ShortKeyFooter {
    UInt32 _num_items;
    UInt32 _key_bytes;
    UInt32 _offset_bytes;
    UInt32 _segment_id;
    UInt32 _num_segment_rows;
};

struct PageFooter {
    PageType _page_type;
    size_t _uncompressed_size;
    DataPageFooter _data_page_footer;
    IndexPageFooter _index_page_footer;
    ShortKeyFooter _short_key_footer;
};

}