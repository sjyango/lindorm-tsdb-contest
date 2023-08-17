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

ColumnWriter::ColumnWriter(ColumnMetaSPtr meta, io::FileWriter* file_writer)
        : _meta(meta), _file_writer(file_writer) {
    _ordinal_index_writer = std::make_unique<OrdinalIndexWriter>();
    switch (_meta->_type->column_type()) {
    case COLUMN_TYPE_STRING:
        _meta->_encoding_type = EncodingType::BINARY_PLAIN_ENCODING;
        _meta->_compression_type = CompressionType::NO_COMPRESSION;
        _page_encoder = std::make_unique<BinaryPlainPageEncoder>();
        break;
    case COLUMN_TYPE_INTEGER:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_DOUBLE_FLOAT:
        _meta->_encoding_type = EncodingType::PLAIN_ENCODING;
        _meta->_compression_type = CompressionType::NO_COMPRESSION;
        _page_encoder = std::make_unique<PlainPageEncoder>(_meta->get_type_size());
        break;
    default:
        _page_encoder = nullptr;
    }
}

ColumnWriter::~ColumnWriter() {
    _page_encoder.reset();
    _data_pages.clear();
    _ordinal_index_writer.reset();
}

void ColumnWriter::append_data(const uint8_t** data, size_t num_rows) {
    size_t remaining = num_rows;
    while (remaining > 0) {
        size_t num_written = remaining;
        _append_data_in_current_page(data, &num_written);
        remaining -= num_written;
        if (_page_encoder->is_page_full()) {
            _flush_current_page();
        }
    }
}

void ColumnWriter::write_column_data() {
    _flush_current_page();

    for (auto& data_page : _data_pages) {
        io::PagePointer page_pointer;
        io::PageIO::write_page(_meta->_encoding_type, _file_writer,
                               data_page._num_items, data_page._page_slices,
                               data_page._footer, &page_pointer);
        _ordinal_index_writer->append_entry(data_page._footer._first_ordinal, page_pointer);
    }
}

void ColumnWriter::write_column_index() {
    std::shared_ptr<OrdinalIndexMeta> ordinal_index_meta = std::make_shared<OrdinalIndexMeta>();
    _ordinal_index_writer->finish(_file_writer, ordinal_index_meta);
    _meta->_indexes.push_back(ordinal_index_meta);
}

void ColumnWriter::_append_data_in_current_page(const uint8_t* data, size_t* num_written) {
    _page_encoder->add(data, num_written);
    _next_ordinal += *num_written;
}

void ColumnWriter::_append_data_in_current_page(const uint8_t** data, size_t* num_written) {
    _append_data_in_current_page(*data, num_written);
    *data += _meta->get_type_size() * (*num_written);
}

void ColumnWriter::_flush_current_page() {
    std::pair<uint32_t, std::vector<Slice>> page_data = std::move(_page_encoder->finish());
    DataPageFooter page_footer(_page_encoder->size() + sizeof(uint32_t),
                               _first_ordinal, _next_ordinal - _first_ordinal);
    EncodedDataPage page;
    page._num_items = page_data.first;
    page._page_slices = std::move(page_data.second);
    page._footer = std::move(page_footer);
    _data_pages.emplace_back(std::move(page));
    _first_ordinal = _next_ordinal;
    _page_encoder->reset();
}

}