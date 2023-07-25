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

#include "storage/delta_writer.h"

namespace LindormContest::storage {

std::unique_ptr<DeltaWriter> DeltaWriter::open(const LindormContest::WriteRequest& w_req, const Schema& schema) {
    return std::move(std::make_unique<DeltaWriter>(w_req, schema));
}

DeltaWriter::DeltaWriter(const WriteRequest& w_req, const Schema& schema) {
    _table_name = w_req.tableName;
    _schema = std::make_unique<TableSchema>(schema);
    _mem_table = std::make_unique<MemTable>(_schema.get());
    _input_block = std::make_unique<vectorized::MutableBlock>(std::move(_schema->create_block()));

    assert(w_req.rows.size() > 0);
    assert(_input_block->columns() == w_req.rows[0].columns.size() + 2); // 2 represents vin + timestamp

    for (const auto& row : w_req.rows) {
        _input_block->add_row(row);
    }
}

DeltaWriter::~DeltaWriter() = default;

Status DeltaWriter::write(const vectorized::Block* block, const std::vector<int>& row_idxs) {
    if (row_idxs.empty()) {
        _mem_table->insert(block);
    } else {
        _mem_table->insert(block, row_idxs);
    }
    if (_mem_table->need_to_flush(MEM_TABLE_FLUSH_THRESHOLD)) {
        auto s = flush_mem_table();
        if (!s.ok()) {
            return s;
        }
    }
    return Status::OK();
}

Status DeltaWriter::append(const vectorized::Block* block) {
    return write(block, {});
}

Status DeltaWriter::flush_mem_table() {
    Status res = _mem_table->flush();
    assert(res.ok());
    _mem_table = std::make_unique<MemTable>(_schema.get()); // reset mem_table
    return res;
}

Status DeltaWriter::close() {
    Status res = _mem_table->flush();
    assert(res.ok());
    _mem_table.reset();
    return res;
}

}