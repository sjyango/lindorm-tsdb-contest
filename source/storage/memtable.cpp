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

#include "storage/memtable.h"

namespace LindormContest::storage {

MemTable::MemTable(TableSchemaSPtr schema, size_t segment_id) : _schema(schema), _segment_id(segment_id) {
    _arena = std::make_unique<Arena>();
    _row_comparator = std::make_unique<RowInBlockComparator>(_schema);
    _skip_list = std::make_unique<VecTable>(_row_comparator.get(), _arena.get());
    vectorized::Block input_block = _schema->create_block();
    _input_mutable_block = vectorized::MutableBlock::build_mutable_block(&input_block);
    vectorized::Block output_block = _schema->create_block();
    _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&output_block);
    _row_comparator->set_block(&_input_mutable_block);
    assert(_input_mutable_block.columns() == _schema->num_columns());
    _segment_writer = std::make_unique<SegmentWriter>(_schema, _segment_id);
}

MemTable::~MemTable() = default;

void MemTable::insert(const vectorized::Block&& input_block) {
    assert(input_block.columns() == _schema->num_columns());
    size_t cursor_in_mutable_block = _input_mutable_block.rows();
    size_t num_rows = input_block.rows();
    _input_mutable_block.add_rows(&input_block, 0, num_rows);

    for (int i = 0; i < num_rows; i++) {
        _row_in_blocks.emplace_back(std::make_unique<RowInBlock>(cursor_in_mutable_block + i));
        bool is_exist = _skip_list->find(_row_in_blocks.back().get(), &_hint);
        if (is_exist) {
            // replace same key
            _hint.curr->key->_row_pos = _row_in_blocks.back()->_row_pos;
        } else {
            _skip_list->insert_with_hint(_row_in_blocks.back().get(), is_exist, &_hint);
            _rows++;
        }
    }
}

void MemTable::flush(size_t* num_rows_written_in_table) {
    VecTable::Iterator it(_skip_list.get());
    vectorized::Block in_block = _input_mutable_block.to_block();
    std::vector<size_t> row_pos_vec;
    row_pos_vec.reserve(in_block.rows());

    for (it.seek_to_first(); it.valid(); it.next()) {
        row_pos_vec.emplace_back(it.key()->_row_pos);
    }

    _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                   row_pos_vec.data() + row_pos_vec.size());
    _segment_writer->append_block(std::move(_output_mutable_block.to_block()),
                                  num_rows_written_in_table);
}

SegmentSPtr MemTable::finalize() {
    return _segment_writer->finalize();
    // KeyBounds key_bounds;
    // key_bounds.min_key = std::move(_segment_writer->min_encoded_key());
    // key_bounds.max_key = std::move(_segment_writer->max_encoded_key());
    // assert(key_bounds.min_key.compare(key_bounds.max_key) <= 0);

    // SegmentStatistics segstat;
    // segstat.row_num = row_num;
    // segstat.data_size = segment_size + (*writer)->get_inverted_index_file_size();
    // segstat.index_size = index_size + (*writer)->get_inverted_index_file_size();
    // segstat.key_bounds = key_bounds;

    // if (flush_size) {
    //     *flush_size = segment_size + index_size;
    // }

    // add_segment(segid, segstat);

}

void MemTable::close() {
    _row_comparator.reset();
    _arena.reset();
    _skip_list.reset();
    _row_in_blocks.clear();
    _segment_writer->close();
    _segment_writer.reset();
}

}