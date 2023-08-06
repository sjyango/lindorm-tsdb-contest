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
#include "io/page_io.h"

namespace LindormContest::storage {

SegmentWriter::SegmentWriter(io::FileWriter* file_writer, TableSchemaSPtr schema, size_t segment_id)
        : _file_writer(file_writer), _schema(schema), _segment_id(segment_id) {
    _num_key_columns = _schema->num_key_columns();
    _num_short_key_columns = _schema->num_short_key_columns();
    _column_writers.reserve(_schema->num_columns());
    _data_convertor.reserve(_schema->num_columns());
    _short_key_index_writer = std::make_unique<ShortKeyIndexWriter>();

    for (const auto& column : _schema->columns()) {
        _create_column_writer(column);
    }

    for (size_t cid = 0; cid < _num_key_columns; ++cid) {
        const auto& column = _schema->column(cid);
        _key_coders.push_back(get_key_coder(column.get_column_type()));
    }
}

SegmentWriter::~SegmentWriter() = default;

void SegmentWriter::_create_column_writer(const TableColumn& column) {
    ColumnMetaSPtr meta = std::make_shared<ColumnMeta>();
    meta->_column_id = column.get_uid();
    meta->_type = column.get_data_type();
    meta->_encoding_type = EncodingType::PLAIN_ENCODING;
    meta->_compression_type = CompressionType::NO_COMPRESSION;
    _footer._column_metas.emplace_back(meta);
    _column_writers.emplace_back(std::make_unique<ColumnWriter>(meta, _file_writer));
    _data_convertor.add_column_data_convertor(column);
}

void SegmentWriter::append_block(vectorized::Block&& block, size_t* num_rows_written_in_table) {
    *num_rows_written_in_table = 0;
    size_t num_rows = block.rows();
    if (num_rows == 0) {
        return;
    }
    _data_convertor.set_source_content(&block, 0, num_rows);
    std::vector<size_t> short_key_pos;

    if (_short_key_row_pos == 0) {
        short_key_pos.push_back(0);
    }

    // 假设数据共5000条，则num_rows=5000，则short_key_pos=[0, 1024, 2048, 3072, 4096]
    while (_short_key_row_pos + NUM_ROWS_PER_GROUP < num_rows) {
        _short_key_row_pos += NUM_ROWS_PER_GROUP;
        short_key_pos.push_back(_short_key_row_pos);
    }

    // convert column data from engine format to storage layer format
    std::vector<ColumnDataConvertor*> key_columns;

    for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
        auto converted_result = _data_convertor.convert_column_data(cid);
        if (cid < _num_key_columns) {
            key_columns.push_back(converted_result);
        }
        const uint8_t* data = reinterpret_cast<const uint8_t*>(converted_result->get_data());
        _column_writers[cid]->append_data(&data, num_rows);
    }

    if (_is_first_row) {
        _min_key = std::move(_encode_keys(key_columns, 0));
        _is_first_row = false;
    }
    _max_key = std::move(_encode_keys(key_columns, num_rows - 1));

    for (const auto pos : short_key_pos) {
        _short_key_index_writer->add_item(_encode_keys(key_columns, pos));
    }

    _num_rows_written = num_rows;
    *num_rows_written_in_table += _num_rows_written;
}

String SegmentWriter::_encode_keys(const std::vector<ColumnDataConvertor*>& key_columns, size_t pos) {
    assert(key_columns.size() == _num_key_columns && _key_coders.size() == _num_key_columns);
    String encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        auto data = column->get_data_at(pos);
        _key_coders[cid]->encode_ascending(data, &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}

void SegmentWriter::_write_short_key_index(io::PagePointer* page_pointer) {
    OwnedSlice short_key_index_body;
    ShortKeyIndexFooter short_key_index_footer;
    _short_key_index_writer->finalize(_num_rows_written, &short_key_index_body, &short_key_index_footer);
    io::PageIO::write_page(_file_writer, std::move(short_key_index_body), short_key_index_footer, page_pointer);
}

void SegmentWriter::close() {
    for (auto& column_writer : _column_writers) {
        column_writer.reset();
    }
    _column_writers.clear();
    _key_coders.clear();
    _short_key_index_writer.reset();
    _data_convertor.reset();
    _num_rows_written = 0;
}

void SegmentWriter::finalize() {
    for (auto& column_writer : _column_writers) {
        column_writer->write_column_data();
    }

    for (auto& column_writer : _column_writers) {
        column_writer->write_column_index();
    }

    io::PagePointer short_key_index_page_pointer;
    _write_short_key_index(&short_key_index_page_pointer);

    _footer._short_key_index_page_pointer = short_key_index_page_pointer;
    _footer._num_rows = _num_rows_written;
    _footer._compression_type = CompressionType::NO_COMPRESSION;

    std::string footer_buffer; // serialize segment footer and footer size
    _footer.serialize(&footer_buffer);
    size_t footer_size = footer_buffer.size();
    put_fixed32_le(&footer_buffer, footer_size);
    _file_writer->append(footer_buffer);
}

}