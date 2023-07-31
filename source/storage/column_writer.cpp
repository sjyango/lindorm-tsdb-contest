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

#include "storage/column_writer.h"

namespace LindormContest::storage {

ColumnWriter::ColumnWriter(const TableColumn& column) {
    ColumnMeta column_meta;
    column_meta._column_id = column.get_uid();
    column_meta._type = column.get_data_type();
    column_meta._ordinal_index = nullptr;
    _column_data = std::make_shared<ColumnData>(std::move(column_meta));
    _ordinal_index_writer = std::make_unique<OrdinalIndexWriter>();
    switch (_column_data->_column_meta.get_column_type()) {
    case COLUMN_TYPE_STRING:
        _page_encoder = std::make_unique<BinaryPlainPageEncoder>();
        break;
    case COLUMN_TYPE_INTEGER:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_DOUBLE_FLOAT:
        _page_encoder = std::make_unique<PlainPageEncoder>(column);
        break;
    default:
        _page_encoder = nullptr;
    }
}

ColumnWriter::~ColumnWriter() = default;

void ColumnWriter::append_data(const uint8_t** data, size_t num_rows) {
    size_t remaining = num_rows;
    while (remaining > 0) {
        size_t num_written = remaining;
        append_data_in_current_page(data, &num_written);
        remaining -= num_written;
        if (_page_encoder->is_page_full()) {
            flush_current_page();
        }
    }
}

void ColumnWriter::append_data_in_current_page(const uint8_t* data, size_t* num_written) {
    _page_encoder->add(data, num_written);
    _next_ordinal += *num_written;
}

void ColumnWriter::append_data_in_current_page(const uint8_t** data, size_t* num_written) {
    append_data_in_current_page(*data, num_written);
    *data += _column_data->get_type_size() * (*num_written);
}

void ColumnWriter::flush_current_page() {
    if (_next_ordinal == _first_ordinal) {
        return;
    }
    OwnedSlice page_data = _page_encoder->finish();
    _page_encoder->reset();
    DataPageMeta data_meta(page_data.size(), _first_ordinal, _next_ordinal - _first_ordinal);
    DataPage page(std::move(page_data), data_meta);
    _data_pages.emplace_back(std::move(page));
    _first_ordinal = _next_ordinal;
}

void ColumnWriter::finish() {
    flush_current_page();
}

ColumnSPtr ColumnWriter::write_data() {
    for (size_t index = 0; index < _data_pages.size(); ++index) {
        _ordinal_index_writer->append_entry(_data_pages[index]._meta._first_ordinal, index);
    }
    _column_data->_column_meta._ordinal_index = _ordinal_index_writer->finalize();
    _column_data->_data_pages = std::move(_data_pages);
    return _column_data;
}

}