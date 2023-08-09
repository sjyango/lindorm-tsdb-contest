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
#include "storage/column_writer.h"
#include "storage/data_convertor.h"
#include "storage/indexs/key_coder.h"
#include "storage/indexs/short_key_index.h"
#include "storage/table_schema.h"
#include "vec/blocks/block.h"

namespace LindormContest::storage {

class SegmentWriter {
    static constexpr size_t NUM_ROWS_PER_GROUP = 1024;

public:
    SegmentWriter(io::FileWriter* file_writer, TableSchemaSPtr schema, size_t segment_id);

    ~SegmentWriter();

    void append_block(vectorized::Block&& block, size_t* num_rows_written);

    void finalize();

    void close();

private:
    void _create_column_writer(const TableColumn& column);

    void _write_short_key_index(io::PagePointer* page_pointer);

    std::string _encode_keys(const std::vector<ColumnDataConvertor*>& key_columns, size_t pos);

    size_t _segment_id;
    io::FileWriter* _file_writer;
    SegmentFooter _footer;
    TableSchemaSPtr _schema;
    size_t _num_key_columns;
    size_t _num_short_key_columns;
    size_t _short_key_row_pos = 0;
    // _num_rows_written means row count already written in this current column group
    size_t _num_rows_written = 0;
    // _row_count means total row count of this segment
    // In vertical compaction row count is recorded when key columns group finish
    //  and _num_rows_written will be updated in value column group
    // size_t _row_count = 0;
    bool _is_first_row = true;
    String _min_key;
    String _max_key;
    BlockDataConvertor _data_convertor;
    std::vector<const KeyCoder*> _key_coders;
    std::vector<std::unique_ptr<ColumnWriter>> _column_writers;
    std::unique_ptr<ShortKeyIndexWriter> _short_key_index_writer;
};

}