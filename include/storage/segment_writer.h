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
#include "common/status.h"
#include "vec/blocks/block.h"
#include "table_schema.h"
#include "column_writer.h"
#include "data_convertor.h"
#include "storage/indexs/short_key_index.h"
#include "storage/indexs/key_coder.h"

namespace LindormContest::storage {

class SegmentWriter {
    static constexpr size_t NUM_ROWS_PER_GROUP = 1024;

public:
    SegmentWriter(const String& root_path, const TableSchema* schema, size_t segment_id);

    ~SegmentWriter();

    size_t num_rows_written() const { return _num_rows_written; }

    size_t row_count() const { return _row_count; }

    Status append_block(const vectorized::Block* block, size_t row_pos, size_t num_rows, size_t* num_rows_written);

    void set_min_key(const Slice& key);

    void set_max_key(const Slice& key);

private:
    void _create_column_writer(const TableColumn& column);

    String _full_encode_keys(const std::vector<ColumnDataConvertor*>& key_columns, size_t pos);

    std::string _encode_keys(const std::vector<ColumnDataConvertor*>& key_columns, size_t pos);

    size_t _segment_id;
    const String& _root_path;
    const TableSchema* _schema;
    size_t _num_key_columns;
    size_t _num_short_key_columns;
    size_t _short_key_row_pos = 0;
    // _num_rows_written means row count already written in this current column group
    size_t _num_rows_written = 0;
    // _row_count means total row count of this segment
    // In vertical compaction row count is recorded when key columns group finish
    //  and _num_rows_written will be updated in value column group
    size_t _row_count = 0;
    bool _is_first_row = true;
    String _min_key;
    String _max_key;
    std::vector<const KeyCoder*> _key_coders;
    std::unique_ptr<ShortKeyIndexBuilder> _short_key_index_builder;
    std::vector<std::unique_ptr<ColumnWriter>> _column_writers;
    std::unique_ptr<BlockDataConvertor> _data_convertor;
};

}