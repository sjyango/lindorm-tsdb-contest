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

#include "common/range.h"
#include "rowwise_iterator.h"
#include "storage/column_reader.h"
#include "storage/indexs/short_key_index.h"
#include "storage/segment_traits.h"

namespace LindormContest::vectorized {
class Block;
}

namespace LindormContest::storage {

class SegmentReader : public RowwiseIterator {
    static constexpr size_t NUM_ROWS_PER_GROUP = 1024;

public:
    SegmentReader(io::FileSystemSPtr fs, const std::string& segment_path,
                  TableSchemaSPtr table_schema, PartialSchemaSPtr schema)
            :  _table_schema(table_schema), _schema(schema) {
        io::FileDescription fd;
        fd._path = segment_path;
        _file_reader = std::move(fs->open_file(fd));
        _parse_footer();
        _load_short_key_index();
        for (const auto& col_id : _schema->column_ids()) {
            _column_readers.emplace(col_id, std::make_unique<ColumnReader>(
                                                    _footer._column_metas[col_id], _footer._num_rows, _file_reader));
        }
        const auto& short_key_column = schema->column(0);
        _short_key_column = vectorized::ColumnFactory::instance().create_column(short_key_column.get_column_type(), short_key_column.get_name());
        _return_columns = std::move(schema->create_block().mutate_columns());
    }

    ~SegmentReader() override = default;

    void next_batch(vectorized::Block* block) override {
        block->clear();
        _read_columns_by_index(_get_row_range_by_key("_key"));
        std::vector<ColumnId> col_ids = _schema->column_ids();
        for (size_t i = 0; i < col_ids.size(); ++i) {
            const TableColumn& column = _schema->column(col_ids[i]);
            block->insert({_return_columns[i], column.get_column_type(), column.get_name()});
        }
        assert(_schema->num_columns() == block->columns());
    }

    PartialSchemaSPtr schema() const override {
        return _schema;
    }

    uint32_t num_rows() const {
        return _footer._num_rows;
    }

private:
    void _parse_footer() {
        // Footer => SegmentFooter + SegmentFooterSize
        size_t file_size = _file_reader->size();
        if (file_size < 4) {
            throw std::runtime_error("Bad segment file");
        }
        uint32_t footer_size;
        size_t bytes_read = 0;
        _file_reader->read_at(file_size - 4, Slice((char*)&footer_size, 4), &bytes_read);
        assert(bytes_read == 4);
        if (file_size < 4 + footer_size) {
            throw std::runtime_error("Bad segment file");
        }
        std::string footer_buffer;
        footer_buffer.resize(footer_size);
        _file_reader->read_at(file_size - 4 - footer_size, footer_buffer, &bytes_read);
        assert(bytes_read == footer_size);
        const uint8_t* footer_start = reinterpret_cast<const uint8_t*>(footer_buffer.c_str());
        _footer.deserialize(footer_start, _table_schema->num_columns());
        assert(_footer._column_metas.size() == _table_schema->num_columns());
    }

    void _load_short_key_index() {
        OwnedSlice data;
        Slice body;
        ShortKeyIndexFooter footer;
        io::PagePointer page_pointer = _footer._short_key_index_page_pointer;
        io::PageIO::read_and_decompress_page(nullptr, page_pointer,
                                             _file_reader, &body, footer, &data);
        assert(footer._page_type == PageType::SHORT_KEY_PAGE);
        _short_key_index_reader = std::make_unique<ShortKeyIndexReader>();
        _short_key_index_reader->load(body, footer);
    }

    // calculate row ranges that fall into requested key using short key index
    RowRange _get_row_range_by_key(const String& key) {
        size_t lower_ordinal = 0;
        size_t upper_ordinal = _lookup_ordinal(key, num_rows(), false);
        if (upper_ordinal > 0) {
            lower_ordinal = _lookup_ordinal(key, upper_ordinal, true);
        }
        return {lower_ordinal, upper_ordinal};
    }

    size_t _lookup_ordinal(const String& key, size_t upper_bound, bool is_include) {
        return _lookup_ordinal_from_short_key_index(key, upper_bound, is_include);
    }

    // lookup the ordinal of given key from short key index
    size_t _lookup_ordinal_from_short_key_index(const String& key, size_t upper_bound, bool is_include) {
        size_t start_group_index;
        auto start_iter = _short_key_index_reader->lower_bound(key);
        if (start_iter.valid()) {
            start_group_index = start_iter.index();
            if (start_group_index > 0) {
                // Because previous block may contain this key, so we should set rowid to
                // last block's first row.
                // 比如某一个ordinal index项为xxx666，但它之间的几条数据也都是xxx666，碰巧查询条件为xxx666，那么需要包含当前group的前一个group的数据
                start_group_index--;
            }
        } else {
            start_group_index = _short_key_index_reader->num_items() - 1;
        }
        size_t start_ordinal = start_group_index * NUM_ROWS_PER_GROUP;
        size_t end_ordinal = upper_bound;
        auto end_iter = _short_key_index_reader->upper_bound(key);
        if (end_iter.valid()) {
            end_ordinal = std::min(upper_bound, end_iter.index() * NUM_ROWS_PER_GROUP);
        }

        // binary search to find the exact key
        while (start_ordinal < end_ordinal) {
            size_t mid_ordinal = (end_ordinal - start_ordinal) / 2 + start_ordinal;
            _seek_and_peek_short_key_column(mid_ordinal); // just seek and peek the vin column (short key)
            const vectorized::ColumnString& short_key_column =
                    reinterpret_cast<const vectorized::ColumnString&>(*_short_key_column);
            int cmp = (short_key_column[0] == key);
            if (cmp > 0) {
                start_ordinal = mid_ordinal + 1;
            } else if (cmp == 0) {
                if (is_include) {
                    // lower bound
                    end_ordinal = mid_ordinal;
                } else {
                    // upper bound
                    start_ordinal = mid_ordinal + 1;
                }
            } else {
                end_ordinal = mid_ordinal;
            }
        }

        return start_ordinal;
    }

    void _seek_and_peek_short_key_column(size_t ordinal) {
        _short_key_column->clear();
        size_t num_rows = 1;
        _column_readers[0]->seek_to_ordinal(ordinal);
        _column_readers[0]->next_batch(&num_rows, _short_key_column);
        assert(num_rows == 0);
    }

    void _read_columns_by_index(const RowRange& range) {
        _seek_columns(_schema->column_ids(), range.from());
        _read_columns(_schema->column_ids(), _return_columns, range.to() - range.from());
    }

    void _seek_columns(const std::vector<ColumnId>& column_ids, size_t ordinal) {
        for (auto col_id : column_ids) {
            assert(_column_readers.find(col_id) != _column_readers.end());
            _column_readers[col_id]->seek_to_ordinal(ordinal);
        }
    }

    void _read_columns(const std::vector<ColumnId>& column_ids,
                       vectorized::SMutableColumns& column_block, size_t num_rows) {
        for (auto cid : column_ids) {
            auto& column = column_block[cid];
            _column_readers[cid]->next_batch(&num_rows, column);
        }
    }

    PartialSchemaSPtr _schema;
    TableSchemaSPtr _table_schema;
    io::FileReaderSPtr _file_reader;
    SegmentFooter _footer;
    std::unordered_map<uint32_t, std::unique_ptr<ColumnReader>> _column_readers;
    std::unique_ptr<ShortKeyIndexReader> _short_key_index_reader;
    ShortKeyIndexFooter _short_key_index_footer;
    vectorized::MutableColumnSPtr _short_key_column;
    vectorized::SMutableColumns _return_columns;
    // const String& _key;
};

}