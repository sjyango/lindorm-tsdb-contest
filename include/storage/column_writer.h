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

#include "Root.h"
#include "page_decoder.h"
#include "segment_traits.h"
#include "storage/indexs/ordinal_key_index.h"
#include "table_schema.h"
#include "vec/blocks/block.h"

namespace LindormContest::storage {

class ColumnWriter {
public:
    ColumnWriter(const TableColumn& column);

    ~ColumnWriter();

    void flush_current_page();

    void append_data(const uint8_t** data, size_t num_rows);

    void append_data_in_current_page(const uint8_t** data, size_t* num_written);

    void append_data_in_current_page(const uint8_t* data, size_t* num_written);

    void finish();

    ColumnSPtr write_data();

private:
    ColumnSPtr _column_data;
    ordinal_t _first_ordinal = 0;
    ordinal_t _next_ordinal = 0;

    std::unique_ptr<PageEncoder> _page_encoder;
    std::vector<DataPage> _data_pages;
    std::unique_ptr<OrdinalIndexWriter> _ordinal_index_writer;
};

}