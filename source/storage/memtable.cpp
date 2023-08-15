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

#include "utils.h"
#include "storage/memtable.h"

namespace LindormContest::storage {

MemTable::MemTable(io::FileWriter* file_writer, TableSchemaSPtr schema, size_t segment_id)
        : _schema(schema), _segment_id(segment_id) {
    _arena = std::make_unique<Arena>();
    _row_comparator = std::make_unique<RowInBlockComparator>(_schema);
    _skip_list = std::make_unique<VecTable>(_row_comparator.get(), _arena.get());
    vectorized::Block input_block = _schema->create_block();
    _input_mutable_block = vectorized::MutableBlock::build_mutable_block(&input_block);
    vectorized::Block output_block = _schema->create_block();
    _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&output_block);
    _row_comparator->set_block(_input_mutable_block.get());
    _segment_writer = std::make_unique<SegmentWriter>(file_writer, _schema, _segment_id);
    assert(_input_mutable_block->columns() == _schema->num_columns());
}

MemTable::~MemTable() {
    _schema.reset();
    _row_comparator.reset();
    _arena.reset();
    _skip_list.reset();
    _row_in_blocks.clear();
    _input_mutable_block.reset();
    _output_mutable_block.reset();
    _segment_writer.reset();
}

void MemTable::insert(const vectorized::Block* input_block) {
    assert(input_block->columns() == _schema->num_columns());
    size_t cursor_in_mutable_block = _input_mutable_block->rows();
    size_t num_rows = input_block->rows();
    _input_mutable_block->append_block(input_block, 0, num_rows);

    for (int i = 0; i < num_rows; i++) {
        _row_in_blocks.emplace_back(std::make_unique<RowInBlock>(cursor_in_mutable_block + i));
        _skip_list->insert(_row_in_blocks.back().get());
        _rows++;
    }
}

void MemTable::flush(size_t* num_rows_written_in_table) {
    if (_rows == 0) {
        *num_rows_written_in_table = 0;
        return;
    }
    VecTable::Iterator it(_skip_list.get());
    vectorized::Block in_block = _input_mutable_block->to_block();
    std::vector<size_t> row_pos_vec;
    row_pos_vec.reserve(in_block.rows());

    for (it.seek_to_first(); it.valid(); it.next()) {
        row_pos_vec.emplace_back(it.key()->_row_pos);
    }

    _output_mutable_block->append_block(&in_block, row_pos_vec.data(), row_pos_vec.data() + row_pos_vec.size());
    vectorized::Block out_block = _output_mutable_block->to_block();
    _segment_writer->append_block(&out_block, num_rows_written_in_table);
}

void MemTable::finalize() {
    _segment_writer->finalize();
}

}