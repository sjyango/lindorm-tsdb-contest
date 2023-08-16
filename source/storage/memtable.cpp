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
        bool is_exist = _skip_list->find(_row_in_blocks.back().get(), &_hint);
        _skip_list->insert_with_hint(_row_in_blocks.back().get(), is_exist, &_hint);
        _rows++;
    }
}

std::optional<std::unordered_map<int32_t, RowPosition>> MemTable::flush(size_t* num_rows_written_in_table) {
    if (_rows == 0) {
        *num_rows_written_in_table = 0;
        return std::nullopt;
    }
    INFO_LOG("Memtable arena size is %zu", _arena->memory_usage())
    VecTable::Iterator it(_skip_list.get());
    vectorized::Block in_block = _input_mutable_block->to_block();
    std::vector<size_t> row_pos_vec;
    row_pos_vec.reserve(in_block.rows());

    for (it.seek_to_first(); it.valid(); it.next()) {
        row_pos_vec.emplace_back(it.key()->_row_pos);
    }

    _output_mutable_block->append_block(&in_block, row_pos_vec.data(), row_pos_vec.data() + row_pos_vec.size());
    vectorized::Block out_block = _output_mutable_block->to_block();
    const vectorized::ColumnInt32& column_vin = reinterpret_cast<const vectorized::ColumnInt32&>(*out_block.get_by_position(0)._column);
    const vectorized::ColumnUInt16& column_timestamp = reinterpret_cast<const vectorized::ColumnUInt16&>(*out_block.get_by_position(1)._column);
    std::unordered_map<int32_t, RowPosition> latest_records;
    int64_t ordinal = column_vin.size() - 1;

    while (ordinal >= 0) {
        latest_records[column_vin[ordinal]] = RowPosition {_segment_id, static_cast<ordinal_t>(ordinal), column_timestamp[ordinal]};
        while (ordinal > 0 && column_vin[ordinal] == column_vin[ordinal - 1]) {
            ordinal--;
        }
        ordinal--;
    }

    INFO_LOG("[segment min] vin: %s, timestamp: %ld", encode_vin(column_vin[0]).vin, encode_timestamp(column_timestamp[0]))
    INFO_LOG("[segment max] vin: %s, timestamp: %ld", encode_vin(column_vin[out_block.rows() - 1]).vin, encode_timestamp(column_timestamp[out_block.rows() - 1]))
    _segment_writer->append_block(&out_block, num_rows_written_in_table);
    return {std::move(latest_records)};
}

void MemTable::finalize() {
    _segment_writer->finalize();
}

}