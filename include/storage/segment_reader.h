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
#include "utils.h"

namespace LindormContest::vectorized {
class Block;
}

namespace LindormContest::storage {

class SegmentReader : public RowwiseIterator {
    static constexpr size_t NUM_ROWS_PER_GROUP = 1024;

public:
    SegmentReader(size_t segment_id, io::FileReaderSPtr file_reader,
                  TableSchemaSPtr table_schema, PartialSchemaSPtr schema)
            : _segment_id(segment_id), _table_schema(table_schema), _schema(schema) {
        _file_reader = file_reader;
        _parse_footer();
        _load_short_key_index();

        for (const auto& col_id : _schema->column_ids()) {
            _column_readers.emplace(col_id, std::make_unique<ColumnReader>(_footer._column_metas[col_id], _footer._num_rows, _file_reader));
        }

        _short_key_columns = std::move(schema->create_block({0, 1}).mutate_columns());
        _return_columns = std::move(schema->create_block().mutate_columns());
    }

    ~SegmentReader() override = default;

    void next_batch(vectorized::Block* block) override {
        block->clear();
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

    RowPosition handle_latest_query(Vin vin) {
        // e.g. xxx -> xxy
        // key = vin + timestamp, e.g. xxx999 -> xxy000
        size_t result_ordinal = _lower_bound(increase_vin(vin), 0);
        // the position we need is `result_ordinal - 1`
        _seek_short_key_columns(result_ordinal - 1);
        const vectorized::ColumnString& column_vin = reinterpret_cast<const vectorized::ColumnString&>(*_short_key_columns[0]);
        const vectorized::ColumnInt64& column_timestamp = reinterpret_cast<const vectorized::ColumnInt64&>(*_short_key_columns[1]);
        assert(column_vin.size() == 1);
        assert(column_timestamp.size() == 1);
        return {_segment_id, column_vin.get(0), column_timestamp.get(0), result_ordinal - 1};
    }

    std::vector<RowPosition> handle_time_range_query(Row lower_bound_row, Row upper_bound_row) {
        std::string lower_vin(lower_bound_row.vin.vin, 17);
        std::string upper_vin(upper_bound_row.vin.vin, 17);
        size_t start_ordinal = _lower_bound(lower_vin, lower_bound_row.timestamp);
        size_t end_ordinal = _lower_bound(upper_vin, upper_bound_row.timestamp);
        // the range we need is [start_ordinal, end_ordinal) aka. [start_ordinal, end_ordinal - 1]
        _read_columns_by_range({0, 1}, start_ordinal, end_ordinal);
        assert(_return_columns[0]->size() == end_ordinal - start_ordinal);
        assert(_return_columns[1]->size() == end_ordinal - start_ordinal);
        std::vector<RowPosition> results;

        for (size_t i = 0; i < end_ordinal - start_ordinal; ++i) {
            const vectorized::ColumnString& column_vin = reinterpret_cast<const vectorized::ColumnString&>(*_return_columns[0]);
            const vectorized::ColumnInt64& column_timestamp = reinterpret_cast<const vectorized::ColumnInt64&>(*_return_columns[1]);
            results.emplace_back(_segment_id, column_vin.get(i), column_timestamp.get(i), start_ordinal + i);
        }

        return std::move(results);
    }

    void seek_to_first() {
        _seek_columns_first(_schema->column_ids());
    }

    void seek_to_ordinal(ordinal_t ordinal) {
        _seek_columns(_schema->column_ids(), ordinal);
    }

    void next_batch(size_t* n, vectorized::Block* dst) {
        _read_columns(_schema->column_ids(), _return_columns, n);
        size_t i = 0;

        for (auto& item : *dst) {
            item._column = std::move(_return_columns[i++]);
        }
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

    // lookup the ordinal of given key from short key index
    // key == vin + timestamp
    size_t _lower_bound(const std::string& vin, int64_t timestamp) {
        std::string key = vin;
        KeyCoderTraits<COLUMN_TYPE_TIMESTAMP>::encode_ascending(&timestamp, &key);
        auto end_iter = _short_key_index_reader->upper_bound(key);
        auto begin_iter = --end_iter;
        size_t start_ordinal = begin_iter.index() * NUM_ROWS_PER_GROUP;
        size_t end_ordinal = end_iter.index() * NUM_ROWS_PER_GROUP;

        // binary search to find the exact key
        while (start_ordinal < end_ordinal) {
            size_t mid_ordinal = (end_ordinal - start_ordinal) / 2 + start_ordinal;
            _seek_short_key_columns(mid_ordinal);
            int cmp = _compare_with_input_key(vin, timestamp);
            if (cmp < 0) {
                start_ordinal = mid_ordinal + 1;
            } else {
                end_ordinal = mid_ordinal;
            }
        }

        return start_ordinal;
    }

    // short key < input key, return -1
    // short key = input key, return 0
    // short key > input key, return 1
    int _compare_with_input_key(const std::string& vin, int64_t timestamp) const {
        const vectorized::ColumnString& column_vin = reinterpret_cast<const vectorized::ColumnString&>(*_short_key_columns[0]);
        const vectorized::ColumnInt64& column_timestamp = reinterpret_cast<const vectorized::ColumnInt64&>(*_short_key_columns[1]);
        assert(column_vin.size() == 1);
        assert(column_timestamp.size() == 1);
        if (column_vin.get(0) < vin) {
            return -1;
        } else if (column_vin.get(0) > vin) {
            return 1;
        } else {
            if (column_timestamp.get(0) < timestamp) {
                return -1;
            } else if (column_timestamp.get(0) > timestamp) {
                return 1;
            }  else {
                return 0;
            }
        }
    }

    // calculate row ranges that fall into requested key using short key index
    // RowRange _lookup_ordinal_range(const String& key, int64_t lower_bound_timestamp, int64_t upper_bound_timestamp) {
    //
    //
    //     size_t lower_ordinal = 0;
    //     size_t upper_ordinal = _lookup_ordinal(key, num_rows(), false);
    //     if (upper_ordinal > 0) {
    //         lower_ordinal = _lookup_ordinal(key, upper_ordinal, true);
    //     }
    //     return {lower_ordinal, upper_ordinal};
    // }

    // lookup the ordinal of given key from short key index
    // key == vin + timestamp
    // bool _lookup_ordinal(const String& vin, int64_t timestamp, size_t* result) {
    //     std::string key = vin;
    //     KeyCoderTraits<COLUMN_TYPE_TIMESTAMP>::encode_ascending(&timestamp, &key);
    //     auto end_iter = _short_key_index_reader->upper_bound(key);
    //     auto begin_iter = --end_iter;
    //     size_t start_ordinal = begin_iter.index() * NUM_ROWS_PER_GROUP;
    //     size_t end_ordinal = end_iter.index() * NUM_ROWS_PER_GROUP - 1;
    //
    //     // binary search to find the exact key
    //     while (start_ordinal <= end_ordinal) {
    //         size_t mid_ordinal = (end_ordinal - start_ordinal) / 2 + start_ordinal;
    //         _seek_short_key_columns(mid_ordinal);
    //         int cmp = _compare_with_input_key(vin, timestamp);
    //         if (cmp == 0) {
    //             *result = mid_ordinal;
    //             return true;
    //         } else if (cmp > 0) {
    //             end_ordinal = mid_ordinal - 1;
    //         } else {
    //             start_ordinal = mid_ordinal + 1;
    //         }
    //     }
    //
    //     return false;
    // }

    // lookup the ordinal of given key from short key index
    // size_t _lookup_ordinal_from_short_key_index(const String& key, size_t upper_bound, bool is_include) {
    //     size_t start_group_index;
    //     auto start_iter = _short_key_index_reader->lower_bound(key);
    //     if (start_iter.valid()) {
    //         start_group_index = start_iter.index();
    //         if (*start_iter == key) {
    //             // Because previous block may contain this key, so we should set rowid to
    //             // last block's first row.
    //             // 比如某一个short index项为xxx666，但它之间的几条数据也都是xxx666，碰巧查询条件为xxx666，那么需要包含当前group的前一个group的数据
    //             start_group_index--;
    //         }
    //     } else {
    //         start_group_index = _short_key_index_reader->num_items() - 1;
    //     }
    //     size_t start_ordinal = start_group_index * NUM_ROWS_PER_GROUP;
    //     size_t end_ordinal = upper_bound;
    //     auto end_iter = _short_key_index_reader->upper_bound(key);
    //     if (end_iter.valid()) {
    //         end_ordinal = std::min(upper_bound, end_iter.index() * NUM_ROWS_PER_GROUP);
    //     }
    //
    //     // binary search to find the exact key
    //     while (start_ordinal < end_ordinal) {
    //         size_t mid_ordinal = (end_ordinal - start_ordinal) / 2 + start_ordinal;
    //         _seek_and_peek_short_key_columns(mid_ordinal);
    //         const vectorized::ColumnString& short_key_column =
    //                 reinterpret_cast<const vectorized::ColumnString&>(*_short_key_column);
    //         int cmp = (short_key_column[0] == key);
    //         if (cmp > 0) {
    //             start_ordinal = mid_ordinal + 1;
    //         } else if (cmp == 0) {
    //             if (is_include) {
    //                 // lower bound
    //                 end_ordinal = mid_ordinal;
    //             } else {
    //                 // upper bound
    //                 start_ordinal = mid_ordinal + 1;
    //             }
    //         } else {
    //             end_ordinal = mid_ordinal;
    //         }
    //     }
    //
    //     return start_ordinal;
    // }

    void _seek_short_key_columns(size_t ordinal) {
        assert(_short_key_columns.size() == 2);
        size_t num_rows = 1;
        _short_key_columns[0]->clear();
        _column_readers[0]->seek_to_ordinal(ordinal);
        _column_readers[0]->next_batch(&num_rows, _short_key_columns[0]);
        _short_key_columns[1]->clear();
        _column_readers[1]->seek_to_ordinal(ordinal);
        _column_readers[1]->next_batch(&num_rows, _short_key_columns[1]);
    }

    void _read_columns_by_index(const RowRange& range) {
        for (auto& column : _return_columns) {
            column->clear();
        }
        size_t num_to_read = range.to() - range.from();
        _seek_columns(_schema->column_ids(), range.from());
        _read_columns(_schema->column_ids(), _return_columns, &num_to_read);
        assert(num_to_read == (range.to() - range.from()));
    }

    // [start_ordinal, end_ordinal)
    void _read_columns_by_range(const std::vector<ColumnId>& column_ids, size_t start_ordinal, size_t end_ordinal) {
        for (auto& column : _return_columns) {
            column->clear();
        }
        size_t num_to_read = end_ordinal - start_ordinal;
        _seek_columns(column_ids, start_ordinal);
        _read_columns(column_ids, _return_columns, &num_to_read);
        assert(num_to_read == (end_ordinal - start_ordinal));
    }

    void _seek_columns_first(const std::vector<ColumnId>& column_ids) {
        for (auto col_id : column_ids) {
            assert(_column_readers.find(col_id) != _column_readers.end());
            _column_readers[col_id]->seek_to_first();
        }
    }

    void _seek_columns(const std::vector<ColumnId>& column_ids, size_t ordinal) {
        for (auto col_id : column_ids) {
            assert(_column_readers.find(col_id) != _column_readers.end());
            _column_readers[col_id]->seek_to_ordinal(ordinal);
        }
    }

    void _read_columns(const std::vector<ColumnId>& column_ids,
                       vectorized::SMutableColumns& column_block, size_t* num_rows) {
        for (auto col_id : column_ids) {
            assert(_column_readers.find(col_id) != _column_readers.end());
            _column_readers[col_id]->next_batch(num_rows, column_block[col_id]);
        }
    }

    size_t _segment_id;
    PartialSchemaSPtr _schema;
    TableSchemaSPtr _table_schema;
    io::FileReaderSPtr _file_reader;
    SegmentFooter _footer;
    std::unordered_map<uint32_t, std::unique_ptr<ColumnReader>> _column_readers;
    std::unique_ptr<ShortKeyIndexReader> _short_key_index_reader;
    vectorized::SMutableColumns _short_key_columns;
    vectorized::SMutableColumns _return_columns;
};

}