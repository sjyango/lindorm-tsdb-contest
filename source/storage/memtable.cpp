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
#include "utils.h"

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
    _row_comparator->set_block(&_input_mutable_block);
    _segment_writer = std::make_unique<SegmentWriter>(file_writer, _schema, _segment_id);
    assert(_input_mutable_block.columns() == _schema->num_columns());
}

MemTable::~MemTable() = default;

void MemTable::insert(const vectorized::Block&& input_block) {
    assert(input_block.columns() == _schema->num_columns());
    size_t cursor_in_mutable_block = _input_mutable_block.rows();
    size_t num_rows = input_block.rows();
    _input_mutable_block.append_block(&input_block, 0, num_rows);

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

    _output_mutable_block.append_block(&in_block, row_pos_vec.data(),
                                   row_pos_vec.data() + row_pos_vec.size());
    _segment_writer->append_block(std::move(_output_mutable_block.to_block()),
                                  num_rows_written_in_table);
}

void MemTable::finalize() {
    _segment_writer->finalize();
}

void MemTable::close() {
    _row_comparator.reset();
    _arena.reset();
    _skip_list.reset();
    _row_in_blocks.clear();
    _segment_writer->close();
    _segment_writer.reset();
}

// row's vin must increase one bit and timestamp must == 0
void MemTable::handle_latest_query(std::vector<Row> rows, vectorized::Block* block) {
    std::vector<size_t> result_positions;

    for (const auto& row : rows) {
        RowInBlock row_in_block(_input_mutable_block.rows());
        _input_mutable_block.add_row(row);

        VecTable::Iterator iter(_skip_list.get());
        std::vector<RowInBlock*> row_in_blocks;

        for (iter.seek_to_first(); iter.valid(); iter.next()) {
            row_in_blocks.emplace_back(iter.key());
        }

        auto cmp = [&] (const RowInBlock* lhs, const RowInBlock* rhs) {
            return (*_row_comparator)(lhs, rhs) < 0;
        };

        auto it = std::lower_bound(row_in_blocks.begin(), row_in_blocks.end(), &row_in_block, cmp);
        auto result_it = it != row_in_blocks.begin() ? --it : row_in_blocks.begin();
        std::string result_str = reinterpret_cast<const vectorized::ColumnString&>(
                                  *_input_mutable_block.get_column_by_position(0)).get((*result_it)->_row_pos);
        if (result_str == decrease_vin(row.vin)) {
            result_positions.push_back((*result_it)->_row_pos);
        }
    }

    for (auto& elem : *block) {
        vectorized::IColumn& col = const_cast<vectorized::IColumn&>(*elem._column);
        col.insert_indices_from(*_input_mutable_block.get_column_by_name(elem._name),
                result_positions.data(),result_positions.data() + result_positions.size());
    }
}

void MemTable::handle_time_range_query(Row lower_bound_row, Row upper_bound_row, vectorized::Block* block) {
    RowInBlock lower_bound_row_in_block(_input_mutable_block.rows());
    _input_mutable_block.add_row(lower_bound_row);
    RowInBlock upper_bound_row_in_block(_input_mutable_block.rows());
    _input_mutable_block.add_row(upper_bound_row);

    VecTable::Iterator iter(_skip_list.get());
    std::vector<RowInBlock*> row_in_blocks;

    for (iter.seek_to_first(); iter.valid(); iter.next()) {
        row_in_blocks.emplace_back(iter.key());
    }

    auto cmp = [&] (const RowInBlock* lhs, const RowInBlock* rhs) {
        return (*_row_comparator)(lhs, rhs) < 0;
    };

    auto lower_bound_it = std::lower_bound(row_in_blocks.begin(), row_in_blocks.end(), &lower_bound_row_in_block, cmp);
    auto upper_bound_it = std::lower_bound(row_in_blocks.begin(), row_in_blocks.end(), &upper_bound_row_in_block, cmp);
    assert(lower_bound_it <= upper_bound_it);

    std::vector<size_t> result_positions;

    for (; lower_bound_it != upper_bound_it; lower_bound_it++) {
        result_positions.push_back((*lower_bound_it)->_row_pos);
    }

    for (auto& elem : *block) {
        vectorized::IColumn& col = const_cast<vectorized::IColumn&>(*elem._column);
        col.insert_indices_from(*_input_mutable_block.get_column_by_name(elem._name),
                                result_positions.data(),result_positions.data() + result_positions.size());
    }
}

}