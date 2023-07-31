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

class SegmentReader {
    static constexpr size_t NUM_ROWS_PER_GROUP = 1024;

public:
    SegmentReader(SegmentSPtr segment_data, SchemaSPtr schema, const String& key)
            : _segment_data(segment_data), _schema(schema), _key(key) {
        _short_key_index_reader = std::make_unique<ShortKeyIndexReader>();
        _short_key_index_reader->parse(_segment_data->_short_key_index_page.get());
        for (const auto& col_id : _schema->column_ids()) {
            _column_readers.emplace(col_id, std::make_shared<ColumnReader>(
                                                  _segment_data->at(col_id), _segment_data->_num_rows));
        }
        const auto& short_key_column = schema->column(0);
        _short_key_column = vectorized::ColumnFactory::instance().create_column(short_key_column.get_column_type(), short_key_column.get_name());
        _return_columns = std::move(schema->create_block().mutate_columns());
    }

    ~SegmentReader() = default;

    void next_batch(vectorized::Block* block) {
        block->clear();
        _read_columns_by_index(_get_row_range_by_key(_key));
        std::vector<ColumnId> col_ids = _schema->column_ids();
        for (size_t i = 0; i < col_ids.size(); ++i) {
            const TableColumn& column = _schema->column(col_ids[i]);
            block->insert({_return_columns[i], column.get_column_type(), column.get_name()});
        }
        assert(_schema->num_columns() == block->columns());
    }

    const Schema& schema() const {
        return *_schema;
    }

    uint64_t segment_id() const {
        return _segment_data->_segment_id;
    }

    uint32_t num_rows() const {
        return _segment_data->_num_rows;
    }

private:
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

    SegmentSPtr _segment_data;
    SchemaSPtr _schema;
    std::unordered_map<ColumnId, std::shared_ptr<ColumnReader>> _column_readers;
    std::unique_ptr<ShortKeyIndexReader> _short_key_index_reader;
    vectorized::MutableColumnSPtr _short_key_column;
    vectorized::SMutableColumns _return_columns;
    const String& _key;
};

}