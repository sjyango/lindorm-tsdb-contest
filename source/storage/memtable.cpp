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

MemTable::MemTable(const TableSchema* schema) : _schema(schema) {
    _arena = std::make_unique<Arena>();
    _row_comparator = std::make_unique<RowInBlockComparator>(_schema);
    _skip_list = std::make_unique<VecTable>(_row_comparator.get(), _arena.get());
    vectorized::Block block = _schema->create_block();
    _input_mutable_block = vectorized::MutableBlock::build_mutable_block(&block);
    _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&block);
    _row_comparator->set_block(&_input_mutable_block);
    assert(_input_mutable_block.columns() == _schema->num_columns());
}

MemTable::~MemTable() = default;

void MemTable::insert(const vectorized::Block&& input_block, const std::vector<int>& row_idxs) {
    assert(input_block.columns() == _schema->num_columns());
    auto num_rows = row_idxs.size();
    size_t cursor_in_mutable_block = _input_mutable_block.rows();

    if (row_idxs.empty()) {
        _input_mutable_block.add_rows(&input_block, 0, num_rows);
    } else {
        _input_mutable_block.add_rows(&input_block, row_idxs.data(), row_idxs.data() + num_rows);
    }

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

void MemTable::_collect_skip_list() {
    VecTable::Iterator it(_skip_list.get());
    vectorized::Block in_block = _input_mutable_block.to_block();
    std::vector<int> row_pos_vec;
    row_pos_vec.reserve(in_block.rows());

    for (it.seek_to_first(); it.valid(); it.next()) {
        row_pos_vec.emplace_back(it.key()->_row_pos);
    }

    _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                   row_pos_vec.data() + in_block.rows());
    _skip_list.reset();
}

bool MemTable::need_to_flush(size_t threshold) const {
    return _rows >= threshold;
}

vectorized::Block MemTable::flush() {
    _collect_skip_list();
    return std::move(_output_mutable_block.to_block());
}

}