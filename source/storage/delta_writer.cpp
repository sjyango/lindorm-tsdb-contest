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

std::unique_ptr<DeltaWriter> DeltaWriter::open(const String& root_path, const String& table_name, const Schema& schema) {
    return std::move(std::make_unique<DeltaWriter>(root_path, table_name, schema));
}

DeltaWriter::DeltaWriter(const String& root_path, const String& table_name, const Schema& schema)
        : _root_path(root_path), _table_name(table_name) {
    _schema = std::make_unique<TableSchema>(schema);
    _mem_table = std::make_unique<MemTable>(_schema.get());
    _segment_writer = std::make_unique<SegmentWriter>(root_path, _schema.get(), allocate_segment_id());
}

DeltaWriter::~DeltaWriter() = default;

Status DeltaWriter::append(const WriteRequest& w_req) {
    assert(w_req.rows.size() > 0);
    _input_block = std::make_unique<vectorized::MutableBlock>(std::move(_schema->create_block()));
    assert(_input_block->columns() == w_req.rows[0].columns.size() + 2); // 2 represents vin + timestamp

    for (const auto& row : w_req.rows) {
        _input_block->add_row(row);
    }

    Status res = write(std::move(_input_block->to_block()), {});
    if(!res.ok()) {
        return res;
    }
    _input_block.reset();
    return Status::OK();
}

Status DeltaWriter::write(const vectorized::Block&& block, const std::vector<int>& row_idxs) {
    _mem_table->insert(std::move(block), row_idxs);
    if (_mem_table->need_to_flush(MEM_TABLE_FLUSH_THRESHOLD)) {
        Status res = flush_mem_table();
        if (!res.ok()) {
            return res;
        }
    }
    return Status::OK();
}

Status DeltaWriter::flush_mem_table() {
    vectorized::Block block = std::move(_mem_table->flush());
    if (block.empty()) {
        return Status::OK();
    }
    _segment_writer->append_block(&block, 0, block.rows(), &_num_rows_written);
    _mem_table = std::make_unique<MemTable>(_schema.get()); // reset mem_table
    _segment_writer = std::make_unique<SegmentWriter>(_root_path, _schema.get(), allocate_segment_id());
    return Status::OK();
}

Status DeltaWriter::close() {
    vectorized::Block block = std::move(_mem_table->flush());
    if (block.empty()) {
        return Status::OK();
    }
    _segment_writer->append_block(&block, 0, block.rows(), &_num_rows_written);
    _mem_table.reset(); // reset mem_table
    _segment_writer.reset(); // reset segment_writer
    return Status::OK();
}

}