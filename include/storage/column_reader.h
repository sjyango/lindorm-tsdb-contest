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

namespace LindormContest::storage {

using OrdinalPageIndexIterator = OrdinalIndexReader::OrdinalPageIndexIterator;

class BaseColumnReader {
public:
    BaseColumnReader() = default;

    virtual ~BaseColumnReader() = default;

    // Seek to the first entry in the column.
    virtual void seek_to_first() {
        throw std::runtime_error("seek_to_first not implement");
    }

    // Seek to the given ordinal entry in the column.
    // Entry 0 is the first entry written to the column.
    virtual void seek_to_ordinal(ordinal_t ord) {
        throw std::runtime_error("seek_to_ordinal not implement");
    }

    virtual void next_batch(size_t* n, vectorized::MutableColumnSPtr& dst) {
        throw std::runtime_error("next_batch not implement");
    }

    virtual void read_by_rowids(const rowid_t* rowids, const size_t count, vectorized::MutableColumnSPtr& dst) {
        throw std::runtime_error("read_by_rowids not implement");
    }

    virtual ordinal_t get_current_ordinal() const {
        return 0;
    }
};

class ColumnReader : public BaseColumnReader {
public:
    ColumnReader(ColumnSPtr column_data, uint64_t num_rows)
            : _column_data(column_data), _num_rows(num_rows),
              _current_ordinal(0), _data_page(nullptr) {
        _ordinal_index_reader = std::make_unique<OrdinalIndexReader>();
        _ordinal_index_reader->parse(_column_data->_column_meta._ordinal_index.get());
        switch (_column_data->_column_meta.get_column_type()) {
        case COLUMN_TYPE_STRING:
            _page_decoder = std::make_unique<BinaryPlainPageDecoder>(_column_data.get());
            break;
        case COLUMN_TYPE_INTEGER:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_DOUBLE_FLOAT:
            _page_decoder = std::make_unique<PlainPageDecoder>(_column_data.get());
            break;
        default:
            _page_decoder = nullptr;
        }
    }

    ~ColumnReader() override = default;

    void seek_to_first() override {
        assert(_ordinal_index_reader->is_parsed());
        _ordinal_index_iter = _ordinal_index_reader->begin();
        assert(_ordinal_index_iter.valid());
        _data_page = _page_decoder->decode(_ordinal_index_iter.index());
        _seek_to_pos_in_page(0);
        _current_ordinal = 0;
    }

    void seek_to_ordinal(ordinal_t ordinal) override {
        // if ordinal is not in current data page
        if (!_data_page || !_data_page->contains(ordinal) || !_ordinal_index_iter.valid()) {
            assert(_ordinal_index_reader->is_parsed());
            _ordinal_index_iter = _ordinal_index_reader->seek_at_or_before(ordinal);
            assert(_ordinal_index_iter.valid());
            _data_page = _page_decoder->decode(_ordinal_index_iter.index());
        }
        _seek_to_pos_in_page(ordinal - _data_page->get_first_ordinal());
        _current_ordinal = ordinal;
    }

    void next_batch(size_t* n, vectorized::MutableColumnSPtr& dst) override {
        dst->reserve(*n);
        size_t remaining = *n;

        while (remaining > 0) {
            if (!_data_page->has_remaining()) {
                bool eos = false;
                _load_next_page(&eos);
                if (eos) {
                    break;
                }
            }
            // number of rows to be read from this page
            size_t num_rows_in_page = std::min(remaining, _data_page->remaining());
            size_t num_rows_to_read = num_rows_in_page;
            _page_decoder->next_batch(&num_rows_to_read, dst);
            assert(num_rows_to_read == num_rows_in_page);
            _data_page->_offset_in_page += num_rows_to_read;
            _current_ordinal += num_rows_to_read;
            remaining -= num_rows_in_page;
        }

        *n -= remaining;
    }

    void read_by_rowids(const rowid_t* rowids, const size_t count, vectorized::MutableColumnSPtr& dst) override {
        size_t remaining = count;
        size_t total_read_count = 0;
        size_t num_rows_to_read = 0;

        while (remaining > 0) {
            seek_to_ordinal(rowids[total_read_count]);
            // number of rows to be read from this page
            num_rows_to_read = std::min(remaining, _data_page->remaining());
            _page_decoder->read_by_rowids(&rowids[total_read_count],_data_page->get_first_ordinal(),
                                          &num_rows_to_read, dst);
            total_read_count += num_rows_to_read;
            remaining -= num_rows_to_read;
        }
    }

    ordinal_t get_current_ordinal() const override {
        return _current_ordinal;
    }

private:
    void _seek_to_pos_in_page(ordinal_t offset_in_page) const {
        if (_data_page->_offset_in_page == offset_in_page) {
            return;
        }
        _page_decoder->seek_to_position_in_page(offset_in_page);
        _data_page->_offset_in_page = offset_in_page;
    }

    void _load_next_page(bool* eos) {
        _ordinal_index_iter.next();
        if (!_ordinal_index_iter.valid()) {
            *eos = true;
            return;
        }
        _data_page = _page_decoder->decode(_ordinal_index_iter.index());
        _seek_to_pos_in_page(0);
        *eos = false;
    }

    ColumnSPtr _column_data;
    uint64_t _num_rows;
    std::unique_ptr<PageDecoder> _page_decoder;
    std::unique_ptr<OrdinalIndexReader> _ordinal_index_reader;
    OrdinalPageIndexIterator _ordinal_index_iter;
    ordinal_t _current_ordinal;
    DataPage* _data_page;
};

class EmptyColumnReader : public BaseColumnReader {
public:
    EmptyColumnReader() = default;

    ~EmptyColumnReader() override = default;

    void seek_to_first() override {
        // do nothing
    }

    void seek_to_ordinal(ordinal_t ord) override {
        // do nothing
    }

    ordinal_t get_current_ordinal() const override {
        return 0;
    }

    virtual void next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
        // do nothing
    }

    virtual void read_by_rowids(const rowid_t* rowids, const size_t count,
                                vectorized::MutableColumnPtr& dst) {
        // do nothing
    }
};

}