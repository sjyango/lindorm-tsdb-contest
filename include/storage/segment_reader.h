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

#include <optional>
#include <cmath>

#include "storage/column_reader.h"
#include "storage/indexs/short_key_index.h"
#include "storage/segment_traits.h"
#include "utils.h"

namespace LindormContest::vectorized {
class Block;
}

namespace LindormContest::storage {

class SegmentReader {
    static constexpr size_t NUM_ROWS_PER_GROUP = 1024;

public:
    SegmentReader(io::FileReaderSPtr file_reader, TableSchemaSPtr table_schema)
            : _table_schema(table_schema) {
        // init file reader
        _file_reader = file_reader;
        // init segment footer
        _parse_footer();
        // init short key index
        _load_short_key_index();
        // init all column readers
        for (const auto& column : _table_schema->columns()) {
            _column_readers.emplace(column.get_uid(), std::make_unique<ColumnReader>(_footer._column_metas[column.get_uid()], _footer._num_rows, _file_reader));
        }
        // init short key columns
        _short_key_columns = _table_schema->create_block({0, 1}).mutate_columns();
    }

    ~SegmentReader() = default;

    std::optional<Row> handle_latest_query(PartialSchemaSPtr schema, Vin key_vin) {
        INFO_LOG("handle_latest_query(PartialSchemaSPtr schema, Vin key_vin)")
        vectorized::SMutableColumns return_columns = std::move(schema->create_block().mutate_columns());
        std::unordered_map<uint32_t, size_t> col_id_to_column_index;
        size_t column_index = 0;

        for (const auto& col_id : schema->column_ids()) {
            col_id_to_column_index[col_id] = column_index++;
        }

        // e.g. xxx -> xxy
        // key = vin + timestamp, e.g. xxx999 -> xxy000
        size_t result_ordinal = _lower_bound(increase_vin(key_vin), 0) - 1;
        // the position we need is `result_ordinal - 1`
        _seek_short_key_columns(result_ordinal < 1 ? 0 : result_ordinal);
        const vectorized::ColumnString& column_vin = reinterpret_cast<const vectorized::ColumnString&>(*_short_key_columns[0]);
        assert(column_vin.size() == 1);
        if (std::strncmp(key_vin.vin, column_vin[0].c_str(), 17) != 0) {
            return std::nullopt;
        }
        _read_columns_by_range(schema->column_ids(), return_columns, col_id_to_column_index, result_ordinal, result_ordinal + 1);
        vectorized::Block block = schema->create_block();
        assert(block.columns() == return_columns.size());

        size_t i = 0;

        for (auto& item : block) {
            item._column = std::move(return_columns[i++]);
        }

        assert(block.rows() <= 1);
        return {std::move(block.to_rows()[0])};
    }

    std::optional<vectorized::Block> handle_time_range_query(PartialSchemaSPtr schema, Vin query_vin, size_t lower_bound_timestamp, size_t upper_bound_timestamp) {
        INFO_LOG("handle_time_range_query(PartialSchemaSPtr schema, Vin query_vin, size_t lower_bound_timestamp, size_t upper_bound_timestamp)")
        vectorized::SMutableColumns return_columns = std::move(schema->create_block().mutate_columns());
        std::unordered_map<uint32_t, size_t> col_id_to_column_index;
        size_t column_index = 0;

        for (const auto& col_id : schema->column_ids()) {
            col_id_to_column_index[col_id] = column_index++;
        }
        std::string key_vin(query_vin.vin, 17);
        size_t start_ordinal = _lower_bound(key_vin, lower_bound_timestamp);
        size_t end_ordinal = _lower_bound(key_vin, upper_bound_timestamp);
        assert(start_ordinal <= end_ordinal);
        // the range we need is [start_ordinal, end_ordinal) aka. [start_ordinal, end_ordinal - 1]
        if (start_ordinal == end_ordinal) {
            return std::nullopt;
        }
        _read_columns_by_range(schema->column_ids(), return_columns, col_id_to_column_index, start_ordinal, end_ordinal);
        assert(return_columns[0]->size() == end_ordinal - start_ordinal);
        vectorized::Block block = schema->create_block();
        assert(block.columns() == return_columns.size());
        size_t i = 0;

        for (auto& item : block) {
            item._column = std::move(return_columns[i++]);
        }

        return {std::move(block)};
    }

    // void seek_to_first() {
    //     _seek_columns_first(_schema->column_ids());
    // }
    //
    // void seek_to_ordinal(ordinal_t ordinal) {
    //     _seek_columns(_schema->column_ids(), ordinal);
    // }
    //
    // void next_batch(size_t* n, vectorized::Block* dst) {
    //     _read_columns(_schema->column_ids(), n);
    //     size_t i = 0;
    //
    //     for (auto& item : *dst) {
    //         item._column = std::move(_return_columns[i++]);
    //     }
    // }

private:
    void _parse_footer() {
        // Footer => SegmentFooter + SegmentFooterSize
        size_t file_size = _file_reader->size();
        if (file_size < 4) {
            ERR_LOG("Bad segment file")
            throw std::runtime_error("Bad segment file");
        }
        uint32_t footer_size;
        size_t bytes_read = 0;
        _file_reader->read_at(file_size - 4, Slice((char*)&footer_size, 4), &bytes_read);
        assert(bytes_read == 4);
        if (file_size < 4 + footer_size) {
            ERR_LOG("Bad segment file")
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
        auto begin_iter = end_iter;
        --begin_iter;
        size_t start_ordinal = std::min(begin_iter.index() * NUM_ROWS_PER_GROUP, (size_t)_footer._num_rows);
        size_t end_ordinal = std::min(end_iter.index() * NUM_ROWS_PER_GROUP, (size_t)_footer._num_rows);

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

    // [start_ordinal, end_ordinal)
    void _read_columns_by_range(const std::vector<ColumnId>& column_ids,
                                vectorized::SMutableColumns& return_columns,
                                std::unordered_map<uint32_t, size_t>& col_id_to_column_index,
                                std::size_t start_ordinal, size_t end_ordinal) {
        size_t num_to_read = end_ordinal - start_ordinal;
        _seek_columns(column_ids, start_ordinal);
        _read_columns(column_ids, return_columns, col_id_to_column_index, &num_to_read);
        assert(num_to_read == (end_ordinal - start_ordinal));
    }

    // void _seek_columns_first(const std::vector<ColumnId>& column_ids) {
    //     for (auto col_id : column_ids) {
    //         assert(_column_readers.find(col_id) != _column_readers.end());
    //         _column_readers[col_id]->seek_to_first();
    //     }
    // }

    void _seek_columns(const std::vector<ColumnId>& column_ids, size_t ordinal) {
        for (auto col_id : column_ids) {
            assert(_column_readers.find(col_id) != _column_readers.end());
            _column_readers[col_id]->seek_to_ordinal(ordinal);
        }
    }

    void _read_columns(const std::vector<ColumnId>& column_ids,
                       vectorized::SMutableColumns& return_columns,
                       std::unordered_map<uint32_t, size_t>& col_id_to_column_index,
                       size_t* num_rows) {
        for (auto col_id : column_ids) {
            assert(_column_readers.find(col_id) != _column_readers.end());
            size_t column_index = col_id_to_column_index[col_id];
            _column_readers[col_id]->next_batch(num_rows, return_columns[column_index]);
        }
    }

    TableSchemaSPtr _table_schema;
    io::FileReaderSPtr _file_reader;
    SegmentFooter _footer;
    std::unordered_map<uint32_t, std::unique_ptr<ColumnReader>> _column_readers;
    std::unique_ptr<ShortKeyIndexReader> _short_key_index_reader;
    vectorized::SMutableColumns _short_key_columns;
};

}