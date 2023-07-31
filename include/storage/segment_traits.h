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

#include "storage/table_schema.h"
#include "common/data_type_factory.h"

namespace LindormContest::storage {

enum class EncodingType {
    UNKNOWN_ENCODING,
    PLAIN_ENCODING
};

enum class CompressionType {
    UNKNOWN_COMPRESSION,
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
    ordinal_t _first_ordinal;
    UInt64 _num_rows;

    DataPageMeta(size_t uncompressed_size, ordinal_t first_ordinal, UInt64 num_rows)
            : PageFooter(PageType::DATA_PAGE, uncompressed_size),
              _first_ordinal(first_ordinal), _num_rows(num_rows) {}
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
    const DataType* _type;
    std::shared_ptr<OrdinalIndexPage> _ordinal_index;
    // EncodingType _encoding_type;
    // CompressionType _compression_type;

    ColumnType get_column_type() const {
        return _type->column_type();
    }

    size_t get_type_size() const {
        return _type->type_size();
    }
};

struct DataPage {
    OwnedSlice _data;
    DataPageMeta _meta;

    DataPage(OwnedSlice&& data, DataPageMeta meta)
            : _data(std::move(data)), _meta(meta) {}

    bool contains(ordinal_t ordinal) const {
        return ordinal >= _meta._first_ordinal && ordinal < (_meta._first_ordinal + _meta._num_rows);
    }

    ordinal_t get_first_ordinal() const {
        return _meta._first_ordinal;
    }
};

struct ColumnData;

using ColumnSPtr = std::shared_ptr<ColumnData>;

struct ColumnData {
    ColumnMeta _column_meta;
    std::vector<DataPage> _data_pages;

    ColumnData(ColumnMeta&& column_meta) : _column_meta(std::move(column_meta)) {}

    ColumnData(ColumnMeta&& column_meta, std::vector<DataPage>&& data_pages)
            : _column_meta(std::move(column_meta)), _data_pages(std::move(data_pages)) {}

    size_t get_type_size() const {
        return _column_meta.get_type_size();
    }
};

struct SegmentData;

using SegmentSPtr = std::shared_ptr<SegmentData>;

struct SegmentData : public std::vector<ColumnSPtr>, public std::enable_shared_from_this<SegmentData> {
    UInt32 _version;
    UInt32 _segment_id;
    UInt32 _num_rows = 0;
    TableSchemaSPtr _table_schema;
    std::shared_ptr<ShortKeyIndexPage> _short_key_index_page;
};

}