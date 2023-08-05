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
#include "common/coding.h"
#include "io/io_utils.h"

namespace LindormContest::storage {

enum class EncodingType {
    UNKNOWN_ENCODING,
    PLAIN_ENCODING
};

enum class CompressionType : uint32_t {
    UNKNOWN_COMPRESSION = 0,
    NO_COMPRESSION = 1
};

enum class PageType : uint32_t {
    UNKNOWN_PAGE = 0,
    DATA_PAGE = 1,
    INDEX_PAGE = 2,
    SHORT_KEY_PAGE = 3
};

struct PageFooter {
    PageType _page_type;
    uint32_t _uncompressed_size;

    PageFooter() = default;

    PageFooter(PageType page_type, uint32_t uncompressed_size)
            : _page_type(page_type), _uncompressed_size(uncompressed_size) {}

    virtual void serialize(std::string* buf) const {
        // 8 Bytes = _page_type(4) + _uncompressed_size(4)
        put_fixed32_le(buf, static_cast<uint32_t>(_page_type));
        put_fixed32_le(buf, _uncompressed_size);
    }

    virtual void deserialize(const uint8_t*& data) {
        _page_type = static_cast<PageType>(*reinterpret_cast<const uint32_t*>(data));
        _uncompressed_size = *reinterpret_cast<const uint32_t*>(data + sizeof(uint32_t));
        data += 2 * sizeof(uint32_t);
    }
};

struct DataPageFooter : public PageFooter {
    ordinal_t _first_ordinal;
    uint32_t _num_rows;

    DataPageFooter() = default;

    DataPageFooter(size_t uncompressed_size, ordinal_t first_ordinal, UInt64 num_rows)
            : PageFooter(PageType::DATA_PAGE, uncompressed_size),
              _first_ordinal(first_ordinal), _num_rows(num_rows) {}

    void serialize(std::string* buf) const override {
        // 20 Bytes = PageFooter(8) + _first_ordinal(8) + _num_rows(4)
        PageFooter::serialize(buf);
        put_fixed64_le(buf, _first_ordinal);
        put_fixed32_le(buf, _num_rows);
    }

    void deserialize(const uint8_t*& data) override {
        PageFooter::deserialize(data);
        _first_ordinal = *reinterpret_cast<const uint64_t*>(data);
        _num_rows = *reinterpret_cast<const uint32_t*>(data + sizeof(uint64_t));
        data += sizeof(uint64_t) + sizeof(uint32_t);
    }
};

struct IndexPageFooter : public PageFooter {
    uint32_t _num_entries;

    IndexPageFooter() = default;

    IndexPageFooter(PageType page_type, size_t uncompressed_size, uint64_t num_entries)
            : PageFooter(page_type, uncompressed_size), _num_entries(num_entries) {}

    void serialize(std::string* buf) const override {
        // 12 Bytes = PageFooter(8) + _num_entries(4)
        PageFooter::serialize(buf);
        put_fixed32_le(buf, _num_entries);
    }

    void deserialize(const uint8_t*& data) override {
        PageFooter::deserialize(data);
        _num_entries = *reinterpret_cast<const uint32_t*>(data);
        data += sizeof(uint32_t);
    }
};

struct ShortKeyIndexFooter : public PageFooter {
    uint32_t _num_items;
    uint32_t _key_bytes;
    uint32_t _offset_bytes;
    uint32_t _num_segment_rows;

    ShortKeyIndexFooter() = default;

    ShortKeyIndexFooter(PageType page_type, size_t uncompressed_size,
                   uint32_t num_items, uint32_t key_bytes,
                   uint32_t offset_bytes, uint32_t num_segment_rows)
            : PageFooter(PageType::SHORT_KEY_PAGE, uncompressed_size), _num_items(num_items),
              _key_bytes(key_bytes), _offset_bytes(offset_bytes),
              _num_segment_rows(num_segment_rows) {}

    void serialize(std::string* buf) const override {
        // 24 Bytes = PageFooter(8) + _num_items(4) + _key_bytes(4) + _offset_bytes(4) + _num_segment_rows(4)
        PageFooter::serialize(buf);
        put_fixed32_le(buf, _num_items);
        put_fixed32_le(buf, _key_bytes);
        put_fixed32_le(buf, _offset_bytes);
        put_fixed32_le(buf, _num_segment_rows);
    }

    void deserialize(const uint8_t*& data) override {
        PageFooter::deserialize(data);
        _num_items = *reinterpret_cast<const uint32_t*>(data + sizeof(uint32_t));
        _key_bytes = *reinterpret_cast<const uint32_t*>(data + 1 * sizeof(uint32_t));
        _offset_bytes = *reinterpret_cast<const uint32_t*>(data + 2 * sizeof(uint32_t));
        _num_segment_rows = *reinterpret_cast<const uint32_t*>(data + 3 * sizeof(uint32_t));
        data += 4 * sizeof(uint32_t);
    }
};

enum class ColumnIndexType : uint32_t {
    UNKNOWN_INDEX_TYPE,
    ORDINAL_INDEX,
    ZONE_MAP_INDEX,
    BITMAP_INDEX,
    BLOOM_FILTER_INDEX
};

struct ColumnIndexMeta {
    ColumnIndexType _type;

    ColumnIndexMeta() = default;

    ColumnIndexMeta(ColumnIndexType type) : _type(type) {}

    virtual void serialize(std::string* buf) const {
        // 4 Bytes = _type(4)
        put_fixed32_le(buf, static_cast<uint32_t>(_type));
    }

    virtual void deserialize(const uint8_t*& data) {
        _type = static_cast<ColumnIndexType>(*reinterpret_cast<const uint32_t*>(data));
        data += sizeof(uint32_t);
    }
};

struct OrdinalIndexMeta : public ColumnIndexMeta {
    io::PagePointer _page_pointer;

    OrdinalIndexMeta() = default;

    OrdinalIndexMeta(io::PagePointer page_pointer)
            : ColumnIndexMeta(ColumnIndexType::ORDINAL_INDEX), _page_pointer(page_pointer) {}

    void serialize(std::string* buf) const override {
        // 16 Bytes = _type(4) + _page_pointer(12)
        ColumnIndexMeta::serialize(buf);
        _page_pointer.serialize(buf);
    }

    void deserialize(const uint8_t*& data) override {
        ColumnIndexMeta::deserialize(data);
        _page_pointer.deserialize(data);
    }
};

// struct ShortKeyIndexPage : public PageFooter {
//     UInt32 _num_items;
//     UInt32 _key_bytes;
//     UInt32 _offset_bytes;
//     UInt32 _segment_id;
//     UInt32 _num_segment_rows;
//     OwnedSlice _data;
//
//     ShortKeyIndexPage(size_t uncompressed_size, UInt32 num_items,
//                       UInt32 key_bytes, UInt32 offset_bytes,
//                       UInt32 segment_id, UInt32 num_segment_rows, OwnedSlice&& data)
//             : PageFooter(PageType::SHORT_KEY_PAGE, uncompressed_size),
//               _num_items(num_items), _key_bytes(key_bytes), _offset_bytes(offset_bytes),
//               _segment_id(segment_id), _num_segment_rows(num_segment_rows), _data(std::move(data)) {}
// };

struct OrdinalIndexPage : public PageFooter {
    ColumnIndexType _index_type;
    UInt32 _num_items;
    OwnedSlice _data;

    OrdinalIndexPage(size_t uncompressed_size, UInt32 num_items, OwnedSlice&& data)
            : PageFooter(PageType::INDEX_PAGE, uncompressed_size),
              _index_type(ColumnIndexType::ORDINAL_INDEX), _num_items(num_items), _data(std::move(data)) {}
};

struct ColumnMeta;

using ColumnMetaSPtr = std::shared_ptr<ColumnMeta>;

struct ColumnMeta {
    uint32_t _column_id;
    const DataType* _type = nullptr;
    EncodingType _encoding_type;
    CompressionType _compression_type;
    std::vector<std::shared_ptr<ColumnIndexMeta>> _indexes;

    ColumnMeta() = default;

    ColumnMeta(uint32_t column_id, const DataType* type,
               EncodingType encoding_type, CompressionType compression_type)
            : _column_id(column_id), _type(type),
              _encoding_type(encoding_type), _compression_type(compression_type) {}

    size_t get_type_size() const {
        return _type->type_size();
    }

    ColumnType get_column_type() const {
        return _type->column_type();
    }

    void serialize(std::string* buf) const {
        put_fixed32_le(buf, _column_id);
        put_fixed32_le(buf, static_cast<uint32_t>(_type->column_type()));
        put_fixed32_le(buf, static_cast<uint32_t>(_encoding_type));
        put_fixed32_le(buf, static_cast<uint32_t>(_compression_type));

        for (const auto& index : _indexes) {
            index->serialize(buf);
        }
    }

    void deserialize(const uint8_t*& data) {
        _column_id = *reinterpret_cast<const uint32_t*>(data);
        _type = DataTypeFactory::instance().get_column_data_type(
                static_cast<ColumnType>(*reinterpret_cast<const uint32_t*>(data + sizeof(uint32_t))));
        _encoding_type = static_cast<EncodingType>(*reinterpret_cast<const uint32_t*>(data + 2 * sizeof(uint32_t)));
        _compression_type = static_cast<CompressionType>(*reinterpret_cast<const uint32_t*>(data + 3 * sizeof(uint32_t)));
        data += 4 * sizeof(uint32_t);

        std::shared_ptr<OrdinalIndexMeta> meta = std::make_shared<OrdinalIndexMeta>();
        meta->deserialize(data);
        _indexes.emplace_back(meta);
    }
};

// struct ColumnMeta {
//     UInt32 _column_id;
//     const DataType* _type;
//     std::shared_ptr<OrdinalIndexPage> _ordinal_index;
//     // EncodingType _encoding_type;
//     // CompressionType _compression_type;
//
//     ColumnType get_column_type() const {
//         return _type->column_type();
//     }
//
//     size_t get_type_size() const {
//         return _type->type_size();
//     }
// };

// struct DataPage {
//     OwnedSlice _data;
//     DataPageFooter _footer;
//
//     DataPage(OwnedSlice&& data, DataPageFooter footer)
//             : _data(std::move(data)), _footer(footer) {}
//
//     bool contains(ordinal_t ordinal) const {
//         return ordinal >= _footer._first_ordinal && ordinal < (_footer._first_ordinal + _footer._num_rows);
//     }
//
//     ordinal_t get_first_ordinal() const {
//         return _footer._first_ordinal;
//     }
// };

struct DataPage {
public:
    DataPage() = default;

    DataPage(size_t size) : _data(size) {}

    DataPage(DataPage&& page)
            : _data(std::move(page._data)), _footer(std::move(page._footer)) {}

    ~DataPage() = default;

    char* data() {
        return (char*)_data.data();
    }

    size_t size() {
        return _data.size();
    }

    Slice slice() const {
        return _data.slice();
    }

    bool contains(ordinal_t ordinal) const {
        return ordinal >= _footer._first_ordinal && ordinal < (_footer._first_ordinal + _footer._num_rows);
    }

    ordinal_t get_first_ordinal() const {
        return _footer._first_ordinal;
    }

    void reset_size(size_t n) {
        assert(n <= _data.size());
        _data.resize(n);
    }

    OwnedSlice _data;
    DataPageFooter _footer;
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

// struct SegmentData;
//
// using SegmentSPtr = std::shared_ptr<SegmentData>;
//
// struct SegmentData : public std::vector<ColumnSPtr>, public std::enable_shared_from_this<SegmentData> {
//     UInt32 _version;
//     UInt32 _segment_id;
//     UInt32 _num_rows = 0;
//     TableSchemaSPtr _table_schema;
//     // std::shared_ptr<ShortKeyIndexPage> _short_key_index_page;
// };

struct SegmentFooter {
    SegmentFooter() = default;

    ~SegmentFooter() = default;

    uint32_t _num_rows;
    CompressionType _compression_type;
    io::PagePointer _short_key_index_page_pointer;
    std::vector<ColumnMetaSPtr> _column_metas;

    void serialize(std::string* buf) const {
        put_fixed32_le(buf, _num_rows);
        put_fixed32_le(buf, static_cast<uint32_t>(_compression_type));
        _short_key_index_page_pointer.serialize(buf);

        for (const auto& column_meta : _column_metas) {
            column_meta->serialize(buf);
        }
    }

    void deserialize(const uint8_t*& data, size_t num_columns) {
        _num_rows = *reinterpret_cast<const uint32_t*>(data);
        _compression_type = static_cast<CompressionType>(*reinterpret_cast<const uint32_t*>(data + sizeof(uint32_t)));
        data += 2 * sizeof(uint32_t);
        _short_key_index_page_pointer.deserialize(data);

        for (int i = 0; i < num_columns; ++i) {
            auto meta = std::make_shared<ColumnMeta>();
            meta->deserialize(data);
            _column_metas.emplace_back(meta);
        }
    }
};

}