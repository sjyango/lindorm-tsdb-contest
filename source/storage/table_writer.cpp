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

#include "storage/table_writer.h"

#include <optional>

namespace LindormContest::storage {

TableWriter::TableWriter(io::FileSystemSPtr fs, io::Path table_path, TableSchemaSPtr schema)
        : _fs(fs), _table_path(table_path), _schema(schema) {
    _init_mem_table();
}

TableWriter::~TableWriter() = default;

void TableWriter::append(const WriteRequest& w_req) {
    std::unique_ptr<vectorized::MutableBlock> input_block =
            std::make_unique<vectorized::MutableBlock>(std::move(_schema->create_block()));
    assert(input_block->columns() == w_req.rows[0].columns.size() + 2); // 2 means vin + timestamp

    for (const auto& row : w_req.rows) {
        input_block->add_row(row);
    }

    write(std::move(input_block->to_block()));
    input_block.reset();
}

bool TableWriter::need_to_flush() {
    return _mem_table->rows() >= MEM_TABLE_FLUSH_THRESHOLD;
}

void TableWriter::write(const vectorized::Block&& block) {
    _mem_table->insert(std::move(block));
    if (need_to_flush()) {
        flush();
    }
}

void TableWriter::flush() {
    size_t num_rows_written_in_table = 0;
    _mem_table->flush(&num_rows_written_in_table);
    if (num_rows_written_in_table == 0) {
        return;
    }
    _mem_table->finalize();
    _init_mem_table();
}

void TableWriter::_init_mem_table() {
    size_t segment_id = _next_segment_id++;
    io::Path segment_path = _table_path / io::Path("segment_" + std::to_string(segment_id) + ".dat");
    _file_writer = _fs->create_file(segment_path);
    _mem_table = std::make_unique<MemTable>(_file_writer.get(), _schema, segment_id);
}

}