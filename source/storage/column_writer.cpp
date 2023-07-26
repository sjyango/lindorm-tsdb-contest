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

ColumnWriter::ColumnWriter(const TableColumn& column) : _column(column) {
    _page_builder = std::make_unique<PlainPageBuilder>(column, DEFAULT_PAGE_SIZE);
    _pages = std::make_unique<PageLinkedList>();
    _ordinal_index_builder = std::make_unique<OrdinalIndexWriter>();
}

ColumnWriter::~ColumnWriter() = default;

Status ColumnWriter::append_data(const uint8_t** data, size_t num_rows) {
    size_t remaining = num_rows;
    while (remaining > 0) {
        size_t num_written = remaining;
        Status res = append_data_in_current_page(data, &num_written);
        if (!res.ok()) {
            return res;
        }
        remaining -= num_written;
        if (_page_builder->is_page_full()) {
            res = finish_current_page();
            if (!res.ok()) {
                return res;
            }
        }
    }
    return Status::OK();
}

Status ColumnWriter::append_data_in_current_page(const uint8_t* data, size_t* num_written) {
    Status res = _page_builder->add(data, num_written);
    if (!res.ok()) {
        return res;
    }
    _next_rowid += *num_written;
    return Status::OK();
}

Status ColumnWriter::append_data_in_current_page(const uint8_t** data, size_t* num_written) {
    Status res = append_data_in_current_page(*data, num_written);
    if (!res.ok()) {
        return res;
    }
    *data += _column.get_type_size() * (*num_written);
    return Status::OK();
}

Status ColumnWriter::finish_current_page() {
    if (_next_rowid == _first_rowid) {
        return Status::OK();
    }
    OwnedSlice page_data = _page_builder->finish();
    _page_builder->reset();

    // prepare data page footer
    Page page;
    page._page_footer._page_type = PageType::DATA_PAGE;
    page._page_footer._uncompressed_size = page_data.size();
    page._page_footer._data_page_footer._first_ordinal = _first_rowid;
    page._page_footer._data_page_footer._num_values = _next_rowid - _first_rowid;
    page._data = std::move(page_data);

    _pages->push_page_back(std::move(page));
    _first_rowid = _next_rowid;
    return Status::OK();
}

// Status ColumnWriter::finish() {
//     Status res = finish_current_page();
//     if (!res.ok()) {
//         return res;
//     }
//     return Status::OK();
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

//Status ColumnWriter::write_data() {
//    Page* page = _pages.head;
//    while (page != nullptr) {
//        RETURN_IF_ERROR(_write_data_page(page));
//        page = page->next;
//    }
//    // write column dict
//    if (_encoding_info->encoding() == DICT_ENCODING) {
//        OwnedSlice dict_body;
//        RETURN_IF_ERROR(_page_builder->get_dictionary_page(&dict_body));
//
//        PageFooterPB footer;
//        footer.set_type(DICTIONARY_PAGE);
//        footer.set_uncompressed_size(dict_body.slice().get_size());
//        footer.mutable_dict_page_footer()->set_encoding(PLAIN_ENCODING);
//
//        PagePointer dict_pp;
//        RETURN_IF_ERROR(PageIO::compress_and_write_page(
//                _compress_codec, _opts.compression_min_space_saving, _file_writer,
//                {dict_body.slice()}, footer, &dict_pp));
//        dict_pp.to_proto(_opts.meta->mutable_dict_page());
//    }
//    return Status::OK();
//}

//Status ColumnWriter::write_ordinal_index() {
//    return _ordinal_index_builder->finish(_file_writer, _opts.meta->add_indexes());
//}

//Status ColumnWriter::_write_data_page(LindormContest::storage::ColumnWriter::Page* page) {
//    PagePointer pp;
//    std::vector<Slice> compressed_body;
//    for (auto& data : page->data) {
//        compressed_body.push_back(data.slice());
//    }
//    RETURN_IF_ERROR(PageIO::write_page(_file_writer, compressed_body, page->footer, &pp));
//    _ordinal_index_builder->append_entry(page->footer.data_page_footer().first_ordinal(), pp);
//    return Status::OK();
//}

}