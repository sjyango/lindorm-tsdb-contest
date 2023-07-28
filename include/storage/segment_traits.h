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

#include <unordered_map>

#include "Root.h"
#include "struct/ColumnValue.h"

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

    // const UInt8* decode_from(const UInt8* data, const UInt8* limit) {
    //     data = decode_varint64_ptr(data, limit, &_offset);
    //     if (data == nullptr) {
    //         return nullptr;
    //     }
    //     return decode_varint32_ptr(data, limit, &_size);
    // }
    //
    // bool decode_from(Slice* input) {
    //     bool result = get_varint64(input, &_offset);
    //     if (!result) {
    //         return false;
    //     }
    //     return get_varint32(input, &_size);
    // }
    //
    // void encode_to(String* dst) const {
    //     put_varint64_varint32(dst, _offset, _size);
    // }

    bool operator==(const PagePointer& other) const {
        return _offset == other._offset && _size == other._size;
    }

    bool operator!=(const PagePointer& other) const { return !(*this == other); }
};

struct PageFooter {
    PageType _page_type;
    size_t _uncompressed_size;

    PageFooter(PageType page_type, size_t uncompressed_size)
            : _page_type(page_type), _uncompressed_size(uncompressed_size) {}
};

struct DataPageMeta : public PageFooter {
    UInt64 _first_ordinal;
    UInt64 _num_values;

    DataPageMeta(size_t uncompressed_size, UInt64 first_ordinal, UInt64 num_values)
            : PageFooter(PageType::DATA_PAGE, uncompressed_size),
              _first_ordinal(first_ordinal), _num_values(num_values) {}
};

struct DataPage {
    OwnedSlice _data;
    DataPageMeta _meta;

    DataPage(OwnedSlice&& data, DataPageMeta meta)
            : _data(std::move(data)), _meta(meta) {}
};

struct IndexPageFooter : public PageFooter {
    enum class IndexPageType {
        UNKNOWN_INDEX_PAGE_TYPE,
        LEAF,
        INTERNAL
    };

    UInt32 _num_entries;
    IndexPageType _type;
};

enum class ColumnIndexType {
    UNKNOWN_INDEX_TYPE,
    ORDINAL_INDEX,
    ZONE_MAP_INDEX,
    BITMAP_INDEX,
    BLOOM_FILTER_INDEX
};

struct ShortKeyIndexPage : public PageFooter {
    UInt32 _num_items;
    UInt32 _key_bytes;
    UInt32 _offset_bytes;
    UInt32 _segment_id;
    UInt32 _num_segment_rows;
    OwnedSlice _data;

    ShortKeyIndexPage(size_t uncompressed_size, UInt32 num_items,
                      UInt32 key_bytes, UInt32 offset_bytes,
                      UInt32 segment_id, UInt32 num_segment_rows, OwnedSlice&& data)
            : PageFooter(PageType::SHORT_KEY_PAGE, uncompressed_size),
              _num_items(num_items), _key_bytes(key_bytes), _offset_bytes(offset_bytes),
              _segment_id(segment_id), _num_segment_rows(num_segment_rows), _data(std::move(data)) {}
};

struct OrdinalIndexPage : public PageFooter {
    ColumnIndexType _index_type;
    UInt32 _num_items;
    OwnedSlice _data;

    OrdinalIndexPage(size_t uncompressed_size, UInt32 num_items, OwnedSlice&& data)
            : PageFooter(PageType::INDEX_PAGE, uncompressed_size),
              _index_type(ColumnIndexType::ORDINAL_INDEX), _num_items(num_items), _data(std::move(data)) {}
};

struct ColumnMeta {
    UInt32 _column_id;
    ColumnType _type;
    UInt32 _type_size;
    EncodingType _encoding_type;
    CompressionType _compression_type;
    std::shared_ptr<OrdinalIndexPage> _ordinal_index;

    ColumnMeta(UInt32 column_id, ColumnType type,
               UInt32 type_size, EncodingType encoding_type,
               CompressionType compression_type,
               std::shared_ptr<OrdinalIndexPage> ordinal_index)
            : _column_id(column_id), _type(type), _type_size(type_size), _encoding_type(encoding_type),
              _compression_type(compression_type), _ordinal_index(ordinal_index) {}
};

struct SegmentMeta {
    UInt32 _version = 1;
    std::unordered_map<UInt32, ColumnMeta> _column_metas; // column_id -> ColumnMeta
    UInt32 _num_rows;
    CompressionType _compression_type;
    std::shared_ptr<ShortKeyIndexPage> _short_key_index;
};

struct SegmentData : public std::vector<std::vector<DataPage>> {
    SegmentMeta _segment_meta;
};

struct KeyBounds {
    String min_key;
    String max_key;
};

}