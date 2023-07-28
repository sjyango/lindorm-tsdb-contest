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
        ColumnMeta meta(
                column.get_uid(),
                column.get_type(),
                column.get_type_size(),
                EncodingType::PLAIN_ENCODING,
                CompressionType::NO_COMPRESSION,
                nullptr
                );
        _segment_data._segment_meta._column_metas.emplace(column.get_uid(), std::move(meta));
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

void SegmentWriter::append_block(vectorized::Block&& block, size_t* num_rows_written) {
    size_t num_rows = block.rows();
    _data_convertor->set_source_content(&block, 0, num_rows);
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

    if (_is_first_row) {
        _min_key = std::move(_full_encode_keys(key_columns, 0));
        _is_first_row = false;
    }

    _max_key = std::move(_full_encode_keys(key_columns, num_rows - 1));

    for (const auto pos : short_key_pos) {
        _short_key_index_writer->add_item(_encode_keys(key_columns, pos));
    }
    *num_rows_written += num_rows;
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

String SegmentWriter::_encode_keys(const std::vector<ColumnDataConvertor*>& key_columns, size_t pos) {
    assert(key_columns.size() == _num_short_key_columns);
    String encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        auto data = column->get_data_at(pos);
        _key_coders[cid]->encode_ascending(data, &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}

void SegmentWriter::finalize_segment_data() {
    for (auto& column_writer : _column_writers) {
        column_writer->finish();
    }

    for (auto& column_writer : _column_writers) {
        _segment_data.emplace_back(std::move(column_writer->write_data()));
    }
}

void SegmentWriter::finalize_segment_index() {
    _segment_data._segment_meta._short_key_index = _short_key_index_writer->finalize(_num_rows_written);
    for (const auto& column_writer : _column_writers) {
        auto it = _segment_data._segment_meta._column_metas.find(column_writer->get_uid());
        if (it == _segment_data._segment_meta._column_metas.end()) {
            throw std::logic_error("column_writer is not found");
        }
        it->second._ordinal_index = column_writer->write_ordinal_index();
    }
}

void SegmentWriter::clear() {
    for (auto& column_writer : _column_writers) {
        column_writer.reset();
    }
    _column_writers.clear();
    _key_coders.clear();
    _short_key_index_writer.reset();
    _data_convertor.reset();
    _num_rows_written = 0;
}

SegmentData SegmentWriter::finalize() {
    finalize_segment_data();
    finalize_segment_index();
    _segment_data._segment_meta._num_rows = _num_rows_written;
    clear();
    return std::move(_segment_data);
}

}