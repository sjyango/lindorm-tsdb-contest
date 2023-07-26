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

#include "storage/segment_writer.h"

namespace LindormContest::storage {

SegmentWriter::SegmentWriter(const String& root_path, const TableSchema* schema, size_t segment_id)
        : _root_path(root_path), _schema(schema), _segment_id(segment_id) {
    _num_key_columns = _schema->num_key_columns();
    _num_short_key_columns = _schema->num_short_key_columns();
    _column_writers.reserve(_schema->num_columns());
    _data_convertor->reserve(schema->num_columns());

    for (const auto& column : _schema->columns()) {
        _create_column_writer(column);
    }

    for (size_t cid = 0; cid < _num_key_columns; ++cid) {
        const auto& column = _schema->column(cid);
        _key_coders.push_back(get_key_coder(column.get_type()));
    }
}

SegmentWriter::~SegmentWriter() = default;

void SegmentWriter::_create_column_writer(const TableColumn& column) {
    std::unique_ptr<ColumnWriter> writer = std::make_unique<ColumnWriter>(column);
    _column_writers.push_back(std::move(writer));
    _data_convertor->add_column_data_convertor(column);
}

Status SegmentWriter::append_block(const vectorized::Block* block, size_t row_pos, size_t num_rows, size_t* num_rows_written) {
    _data_convertor->set_source_content(block, row_pos, num_rows);

    // find all row pos for short key indexes
    std::vector<size_t> short_key_pos;

    if (_short_key_row_pos == 0 && *num_rows_written == 0) {
        short_key_pos.push_back(0);
    }

    while (_short_key_row_pos + NUM_ROWS_PER_GROUP < *num_rows_written + num_rows) {
        _short_key_row_pos += NUM_ROWS_PER_GROUP;
        short_key_pos.push_back(_short_key_row_pos - *num_rows_written);
    }

    // convert column data from engine format to storage layer format
    std::vector<ColumnDataConvertor*> key_columns;
    key_columns.resize(_num_short_key_columns);

    for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
        auto converted_result = _data_convertor->convert_column_data(cid);
        if (cid < _num_short_key_columns) {
            key_columns[cid] = converted_result;
        }
        const UInt8* data = reinterpret_cast<const UInt8*>(converted_result->get_data());
        _column_writers[cid]->append_data(&data, num_rows);
    }

    set_min_key(_full_encode_keys(key_columns, 0));
    set_max_key(_full_encode_keys(key_columns, num_rows - 1));
    Status res;

    for (const auto pos : short_key_pos) {
        res = _short_key_index_builder->add_item(_encode_keys(key_columns, pos));
        if (!res.ok()) {
            return res;
        }
    }

    *num_rows_written += num_rows;
    _data_convertor->clear_source_content();
    return Status::OK();
}

void SegmentWriter::set_min_key(const Slice& key) {
    if (_is_first_row) {
        _min_key.append(key.data(), key.size());
        _is_first_row = false;
    }
}

void SegmentWriter::set_max_key(const Slice& key) {
    _max_key.clear();
    _max_key.append(key.data(), key.size());
}

String SegmentWriter::_full_encode_keys(const std::vector<ColumnDataConvertor*>& key_columns, size_t pos) {
    assert(key_columns.size() == _num_key_columns);
    assert(key_columns.size() == _num_key_columns && _key_coders.size() == _num_key_columns);
    std::string encoded_keys;
    size_t cid = 0;

    for (const auto& column : key_columns) {
        auto data = column->get_data_at(pos);
        _key_coders[cid]->full_encode_ascending(data, &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}

std::string SegmentWriter::_encode_keys(const std::vector<ColumnDataConvertor*>& key_columns, size_t pos) {
    assert(key_columns.size() == _num_short_key_columns);
    std::string encoded_keys;
    size_t cid = 0;

    for (const auto& column : key_columns) {
        auto data = column->get_data_at(pos);
        _key_coders[cid]->encode_ascending(data, &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}

}