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
#include "storage/indexs/index_page.h"
#include "storage/indexs/key_coder.h"
#include "io/page_io.h"

namespace LindormContest::storage {

// Ordinal index is implemented by one IndexPage that stores
// the `first value ordinal` and `file pointer` for each data page.
class OrdinalIndexWriter {
public:
    OrdinalIndexWriter() {
        _page_encoder = std::make_unique<IndexPageEncoder>();
    }

    ~OrdinalIndexWriter() = default;

    void append_entry(ordinal_t ordinal, const io::PagePointer& page_pointer) {
        std::string key;
        KeyCoderTraits<COLUMN_TYPE_UNINITIALIZED>::encode_ascending(&ordinal, &key);
        _page_encoder->add(key, page_pointer);
    }

    size_t size() const {
        return _page_encoder->size();
    }

    void finish(io::FileWriter* file_writer, std::shared_ptr<OrdinalIndexMeta> meta) {
        assert(_page_encoder->count() > 0);
        meta->_type = ColumnIndexType::ORDINAL_INDEX;
        OwnedSlice page_body;
        IndexPageFooter page_footer;
        _page_encoder->finish(&page_body, &page_footer);
        io::PagePointer page_pointer;
        io::PageIO::write_page(file_writer, std::move(page_body), page_footer, &page_pointer);
        meta->_page_pointer = page_pointer;
    }

private:
    std::unique_ptr<IndexPageEncoder> _page_encoder;
};

class OrdinalIndexReader {
public:
    class OrdinalPageIndexIterator {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type = int32_t;
        using pointer = int32_t*;
        using reference = int32_t&;
        using difference_type = int32_t;

        OrdinalPageIndexIterator() : _index(nullptr), _cur_idx(-1) {}

        OrdinalPageIndexIterator(const OrdinalIndexReader* index) : _index(index), _cur_idx(0) {}

        OrdinalPageIndexIterator(const OrdinalIndexReader* index, int cur_idx)
                : _index(index), _cur_idx(cur_idx) {}

        bool valid() const {
            return _index != nullptr && _cur_idx < _index->_num_pages;
        }

        void next() {
            assert(_cur_idx < _index->_num_pages);
            _cur_idx++;
        }

        int32_t page_index() const {
            return _cur_idx;
        }

        io::PagePointer page_pointer() const {
            return _index->_pages[_cur_idx];
        }

        ordinal_t first_ordinal() const {
            return _index->get_first_ordinal(_cur_idx);
        }

        ordinal_t last_ordinal() const {
            return _index->get_last_ordinal(_cur_idx);
        }

        bool operator==(const OrdinalPageIndexIterator& other) {
            return _cur_idx == other._cur_idx && _index == other._index;
        }

        bool operator!=(const OrdinalPageIndexIterator& other) {
            return _cur_idx != other._cur_idx || _index != other._index;
        }

        OrdinalPageIndexIterator& operator++() {
            _cur_idx++;
            return *this;
        }

        OrdinalPageIndexIterator& operator--() {
            _cur_idx--;
            return *this;
        }

        OrdinalPageIndexIterator& operator-=(int32_t step) {
            _cur_idx -= step;
            return *this;
        }

        OrdinalPageIndexIterator& operator+=(int32_t step) {
            _cur_idx += step;
            return *this;
        }

        int32_t operator-(const OrdinalPageIndexIterator& other) const {
            return _cur_idx - other._cur_idx;
        }

        ordinal_t operator*() const {
            return _index->_ordinals[_cur_idx];
        }

    private:
        const OrdinalIndexReader* _index;
        int32_t _cur_idx;
    };

    explicit OrdinalIndexReader() = default;

    void load(io::FileReaderSPtr file_reader, const OrdinalIndexMeta& index_meta, ordinal_t num_values) {
        OwnedSlice index_data;
        Slice body;
        IndexPageFooter footer;
        io::PageIO::read_and_decompress_page(
                nullptr, index_meta._page_pointer,
                file_reader, &body, footer, &index_data);
        // parse and save all (ordinal, pp) from index page
        IndexPageDecoder decoder;
        decoder.load(body, footer);
        _num_pages = decoder.count();
        _ordinals.resize(_num_pages);
        _pages.resize(_num_pages);
        //_ordinals.resize(_num_pages + 1);

        for (int i = 0; i < _num_pages; i++) {
            Slice key = decoder.get_key(i);
            ordinal_t ordinal = 0;
            KeyCoderTraits<COLUMN_TYPE_UNINITIALIZED>::decode_ascending(
                    &key, sizeof(ordinal_t), (uint8_t*)&ordinal);
            _ordinals[i] = ordinal;
            _pages[i] = decoder.get_value(i);
        }

        // _ordinals[_num_pages] = num_values;
    }

    // the returned iter points to the largest element which is less than `ordinal`,
    // or points to the first element if all elements are greater than `ordinal`,
    // or points to "end" if all elements are smaller than `ordinal`.
    OrdinalPageIndexIterator seek_at_or_before(ordinal_t ordinal) {
        auto iter = _upper_bound(ordinal);
        if (iter != begin()) {
            --iter;
        }
        return iter;
    }

    inline OrdinalPageIndexIterator begin() const {
        return {this, 0};
    }

    inline OrdinalPageIndexIterator end() const {
        return {this, _num_pages};
    }

    ordinal_t get_first_ordinal(int page_index) const {
        return _ordinals[page_index];
    }

    ordinal_t get_last_ordinal(int page_index) const {
        return get_first_ordinal(page_index + 1) - 1;
    }

    // for test
    int32_t num_data_pages() const {
        return _num_pages;
    }

private:
    friend OrdinalPageIndexIterator;

    OrdinalPageIndexIterator _upper_bound(ordinal_t ordinal) const {
        auto cmp = [](const ordinal_t& lhs, const ordinal_t& rhs) {
            return lhs < rhs;
        };
        return std::upper_bound(begin(), end(), ordinal, cmp);
    }

    int _num_pages = 0;
    std::vector<ordinal_t> _ordinals;
    std::vector<io::PagePointer> _pages;
};

}