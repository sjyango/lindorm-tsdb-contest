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
#include "storage/page_decoder.h"
#include "storage/segment_traits.h"
#include "storage/indexs/ordinal_key_index.h"
#include "storage/table_schema.h"
#include "vec/blocks/block.h"
#include "io/compression.h"

namespace LindormContest::storage {

class ColumnWriter {
public:
    ColumnWriter(ColumnMetaSPtr meta, io::FileWriter* file_writer);

    ~ColumnWriter();

    void append_data(const uint8_t** data, size_t num_rows);

    void write_column_data();

    void write_column_index();

private:
    void _append_data_in_current_page(const uint8_t** data, size_t* num_written);

    void _append_data_in_current_page(const uint8_t* data, size_t* num_written);

    void _flush_current_page();

    ColumnMetaSPtr _meta;
    io::FileWriter* _file_writer;
    io::CompressionUtil* _compression_util;
    ordinal_t _first_ordinal = 0;
    ordinal_t _next_ordinal = 0;
    std::unique_ptr<PageEncoder> _page_encoder;
    std::vector<EncodedDataPage> _data_pages;
    std::unique_ptr<OrdinalIndexWriter> _ordinal_index_writer;
};

}