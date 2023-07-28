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

#include "storage/delta_writer.h"

namespace LindormContest::storage {

std::unique_ptr<DeltaWriter> DeltaWriter::open(
        const String& root_path, const String& table_name,
        const Schema& schema, std::unordered_map<std::string, std::vector<SegmentData>>* segment_datas) {
    return std::move(std::make_unique<DeltaWriter>(root_path, table_name, schema, segment_datas));
}

DeltaWriter::DeltaWriter(const String& root_path, const String& table_name,
                         const Schema& schema, std::unordered_map<std::string, std::vector<SegmentData>>* segment_datas)
        : _root_path(root_path), _table_name(table_name), _segment_datas(segment_datas) {
    _schema = std::make_unique<TableSchema>(schema);
    _mem_table = std::make_unique<MemTable>(_schema.get());
    _segment_writer = std::make_unique<SegmentWriter>(root_path, _schema.get(), allocate_segment_id());
}

DeltaWriter::~DeltaWriter() = default;

void DeltaWriter::append(const WriteRequest& w_req) {
    std::unique_ptr<vectorized::MutableBlock> input_block =
            std::make_unique<vectorized::MutableBlock>(std::move(_schema->create_block()));
    assert(input_block->columns() == w_req.rows[0].columns.size() + 2); // 2 means vin + timestamp

    for (const auto& row : w_req.rows) {
        input_block->add_row(row);
    }

    write(std::move(input_block->to_block()), {});
    input_block.reset();
}

bool DeltaWriter::need_to_flush() {
    return _mem_table->rows() >= MEM_TABLE_FLUSH_THRESHOLD;
}

void DeltaWriter::write(const vectorized::Block&& block, const std::vector<size_t>& row_idxs) {
    _mem_table->insert(std::move(block), row_idxs);
    if (need_to_flush()) {
        flush();
    }
}

void DeltaWriter::reset() {
    _mem_table = std::make_unique<MemTable>(_schema.get()); // reset mem_table
    _segment_writer = std::make_unique<SegmentWriter>(_root_path, _schema.get(), allocate_segment_id());
}

void DeltaWriter::flush() {
    vectorized::Block block = std::move(_mem_table->flush());
    if (block.empty()) {
        return;
    }
    _segment_writer->append_block(std::move(block), &_num_rows_written);
    flush_segment_writer();
    reset();
}

void DeltaWriter::close() {
    vectorized::Block block = std::move(_mem_table->flush());
    if (block.empty()) {
        return;
    }
    _segment_writer->append_block(std::move(block), &_num_rows_written);
    _mem_table.reset(); // reset mem_table
    _segment_writer.reset(); // reset segment_writer
}

void DeltaWriter::flush_segment_writer() {
    size_t segment_id = _segment_writer->segment_id();
    size_t num_rows = _segment_writer->num_rows_written();
    if (num_rows == 0) {
        return;
    }
    // size_t segment_size;
    // size_t index_size;
    (*_segment_datas)[_table_name].emplace_back(std::move(_segment_writer->finalize()));
    // if (!res.ok()) {
    //     return void::Corruption("failed to finalize segment");
    // }

    KeyBounds key_bounds;
    key_bounds.min_key = std::move(_segment_writer->min_encoded_key());
    key_bounds.max_key = std::move(_segment_writer->max_encoded_key());
    assert(key_bounds.min_key.compare(key_bounds.max_key) <= 0);

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

}