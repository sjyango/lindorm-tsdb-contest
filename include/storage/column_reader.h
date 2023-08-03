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

#include "page_decoder.h"
#include "segment_traits.h"
#include "storage/indexs/ordinal_key_index.h"
#include "io/page_io.h"

namespace LindormContest::storage {

using OrdinalPageIndexIterator = OrdinalIndexReader::OrdinalPageIndexIterator;

class ColumnReader {
public:
    ColumnReader(ColumnMetaSPtr meta, uint64_t num_rows, io::FileReaderSPtr file_reader)
            : _meta(meta), _num_rows(num_rows), _file_reader(file_reader),
              _current_ordinal(0), _offset_in_page(0) {
        for (const auto& index_meta : _meta->_indexes) {
            switch (index_meta._type) {
            case ColumnIndexType::ORDINAL_INDEX: {
                _ordinal_index_meta = reinterpret_cast<const OrdinalIndexMeta*>(&index_meta);
                _ordinal_index_reader = std::make_unique<OrdinalIndexReader>();
                _ordinal_index_reader->load(_file_reader, _ordinal_index_meta, _num_rows);
                break;
            }
            default:
                throw std::runtime_error("invalid column index type");
            }
        }

        if (_ordinal_index_meta == nullptr && !is_empty()) {
            throw std::runtime_error("missing ordinal index for column");
        }

        io::get_compression_util(_meta->_compression_type, &_compression_util);

        switch (_meta->get_column_type()) {
        case COLUMN_TYPE_STRING:
            _page_decoder = std::make_unique<BinaryPlainPageDecoder>();
            break;
        case COLUMN_TYPE_INTEGER:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_DOUBLE_FLOAT:
            _page_decoder = std::make_unique<PlainPageDecoder>(meta->_type);
            break;
        default:
            _page_decoder = nullptr;
        }
    }

    ~ColumnReader() = default;

    bool is_empty() const {
        return _num_rows == 0;
    }

    void seek_to_first() {
        _ordinal_index_iter = _ordinal_index_reader->begin();
        assert(_ordinal_index_iter.valid());
        _load_data_page(_ordinal_index_iter.page_pointer());
        _current_ordinal = 0;
    }

    void seek_to_ordinal(ordinal_t ordinal) {
        // if ordinal is not in current data page
        if (!_data_page.contains(ordinal) || !_ordinal_index_iter.valid()) {
            _ordinal_index_iter = _ordinal_index_reader->seek_at_or_before(ordinal);
            assert(_ordinal_index_iter.valid());
            _load_data_page(_ordinal_index_iter.page_pointer());
        }
        _seek_to_pos_in_page(ordinal - _data_page.get_first_ordinal());
        _current_ordinal = ordinal;
    }

    void next_batch(size_t* n, vectorized::MutableColumnSPtr& dst) {
        dst->reserve(*n);
        size_t remaining = *n;

        while (remaining > 0) {
            if (!_has_remaining()) {
                bool eos = false;
                _load_next_page(&eos);
                if (eos) {
                    break;
                }
            }
            size_t num_rows_in_page = std::min(remaining, _remaining());
            size_t num_rows_to_read = num_rows_in_page;
            _page_decoder->next_batch(&num_rows_to_read, dst);
            assert(num_rows_to_read == num_rows_in_page);
            _offset_in_page += num_rows_to_read;
            _current_ordinal += num_rows_to_read;
            remaining -= num_rows_in_page;
        }

        *n -= remaining;
    }

    // void read_by_rowids(const rowid_t* rowids, const size_t count, vectorized::MutableColumnSPtr& dst) {
    //     size_t remaining = count;
    //     size_t total_read_count = 0;
    //     size_t num_rows_to_read = 0;
    //
    //     while (remaining > 0) {
    //         seek_to_ordinal(rowids[total_read_count]);
    //         // number of rows to be read from this page
    //         num_rows_to_read = std::min(remaining, _remaining());
    //         _page_decoder->read_by_rowids(&rowids[total_read_count],_data_page->get_first_ordinal(),
    //                                       &num_rows_to_read, dst);
    //         total_read_count += num_rows_to_read;
    //         remaining -= num_rows_to_read;
    //     }
    // }

    ordinal_t get_current_ordinal() const {
        return _current_ordinal;
    }

private:
    void _seek_to_pos_in_page(ordinal_t offset_in_page) {
        if (_offset_in_page == offset_in_page) {
            return;
        }
        _page_decoder->seek_to_position_in_page(offset_in_page);
        _offset_in_page = offset_in_page;
    }

    void _load_data_page(const io::PagePointer& page_pointer) {
        io::PageIO::read_and_decompress_page(
                _compression_util, page_pointer, _file_reader,
                &_page_body, _data_page._footer, &_data_page._data);
        _page_decoder->init(_page_body);
        _seek_to_pos_in_page(0);
    }

    void _load_next_page(bool* eos) {
        _ordinal_index_iter.next();
        if (!_ordinal_index_iter.valid()) {
            *eos = true;
            return;
        }
        _load_data_page(_ordinal_index_iter.page_pointer());
        *eos = false;
    }

    bool _has_remaining() const {
        return _offset_in_page < _data_page._footer._num_rows;
    }

    size_t _remaining() const {
        return _data_page._footer._num_rows - _offset_in_page;
    }

    ColumnMetaSPtr _meta;
    DataPage _data_page;
    Slice _page_body;
    io::CompressionUtil* _compression_util = nullptr;
    uint64_t _num_rows;
    io::FileReaderSPtr _file_reader;
    std::unique_ptr<PageDecoder> _page_decoder;
    const OrdinalIndexMeta* _ordinal_index_meta = nullptr;
    std::unique_ptr<OrdinalIndexReader> _ordinal_index_reader;
    OrdinalPageIndexIterator _ordinal_index_iter;
    ordinal_t _current_ordinal; // global ordinal
    ordinal_t _offset_in_page; // local ordinal in page
};

// class BaseColumnReader {
// public:
//     BaseColumnReader() = default;
//
//     virtual ~BaseColumnReader() = default;
//
//     // Seek to the first entry in the column.
//     virtual void seek_to_first() {
//         throw std::runtime_error("seek_to_first not implement");
//     }
//
//     // Seek to the given ordinal entry in the column.
//     // Entry 0 is the first entry written to the column.
//     virtual void seek_to_ordinal(ordinal_t ord) {
//         throw std::runtime_error("seek_to_ordinal not implement");
//     }
//
//     virtual void next_batch(size_t* n, vectorized::MutableColumnSPtr& dst) {
//         throw std::runtime_error("next_batch not implement");
//     }
//
//     virtual void read_by_rowids(const rowid_t* rowids, const size_t count, vectorized::MutableColumnSPtr& dst) {
//         throw std::runtime_error("read_by_rowids not implement");
//     }
//
//     virtual ordinal_t get_current_ordinal() const {
//         return 0;
//     }
// };

// class EmptyColumnReader : public BaseColumnReader {
// public:
//     EmptyColumnReader() = default;
//
//     ~EmptyColumnReader() override = default;
//
//     void seek_to_first() override {
//         // do nothing
//     }
//
//     void seek_to_ordinal(ordinal_t ord) override {
//         // do nothing
//     }
//
//     ordinal_t get_current_ordinal() const override {
//         return 0;
//     }
//
//     virtual void next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
//         // do nothing
//     }
//
//     virtual void read_by_rowids(const rowid_t* rowids, const size_t count,
//                                 vectorized::MutableColumnPtr& dst) {
//         // do nothing
//     }
// };

}