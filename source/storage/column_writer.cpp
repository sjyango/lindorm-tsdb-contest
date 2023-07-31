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
    _page_encoder = std::make_unique<PlainPageEncoder>(column, DEFAULT_PAGE_SIZE);
    _ordinal_index_writer = std::make_unique<OrdinalIndexWriter>();
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
    _next_rowid += *num_written;
}

void ColumnWriter::append_data_in_current_page(const uint8_t** data, size_t* num_written) {
    append_data_in_current_page(*data, num_written);
    *data += _column_data->get_type_size() * (*num_written);
}

void ColumnWriter::flush_current_page() {
    if (_next_rowid == _first_rowid) {
        return;
    }
    OwnedSlice page_data = _page_encoder->finish();
    _page_encoder->reset();
    DataPageMeta data_meta(page_data.size(), _first_rowid, _next_rowid - _first_rowid);
    DataPage page(std::move(page_data), data_meta);
    _data_pages.emplace_back(std::move(page));
    _first_rowid = _next_rowid;
}

void ColumnWriter::finish() {
    flush_current_page();
}

ColumnSPtr ColumnWriter::write_data() {
    for (size_t index = 0; index < _data_pages.size(); ++index) {
        const auto& page = _data_pages[index];
        _ordinal_index_writer->append_entry(page._meta._first_ordinal, index);
    }
    _column_data->_column_meta._ordinal_index = _ordinal_index_writer->finalize();
    _column_data->_data_pages = std::move(_data_pages);
    return _column_data;
}

// write a data page into file and update ordinal index
// Status ColumnWriter::_write_data_page(Page* page) {
//    PagePointer pp;
//    std::vector<Slice> compressed_body;
//    for (auto& data : page->data) {
//        compressed_body.push_back(data.slice());
//    }
//    RETURN_IF_ERROR(PageIO::write_page(_file_writer, compressed_body, page->footer, &pp));
//    _ordinal_index_builder->append_entry(page->footer.data_page_footer().first_ordinal(), pp);
//    return Status::OK();
// }

//uint64_t ColumnWriter::estimate_buffer_size() {
//    uint64_t size = _data_size;
//    size += _page_builder->size();
//    if (is_nullable()) {
//        size += _null_bitmap_builder->size();
//    }
//    size += _ordinal_index_builder->size();
//    if (_opts.need_zone_map) {
//        size += _zone_map_index_builder->size();
//    }
//    if (_opts.need_bitmap_index) {
//        size += _bitmap_index_builder->size();
//    }
//    if (_opts.need_bloom_filter) {
//        size += _bloom_filter_index_builder->size();
//    }
//    return size;
//}

}