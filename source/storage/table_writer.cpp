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

TableWriter::TableWriter(io::FileSystemSPtr fs, TableSchemaSPtr schema, size_t MEM_TABLE_FLUSH_THRESHOLD)
        : _fs(fs), _schema(schema), _MEM_TABLE_FLUSH_THRESHOLD(MEM_TABLE_FLUSH_THRESHOLD), _inited(false) {
    _init_mem_table();
}

TableWriter::~TableWriter() = default;

void TableWriter::append(const std::vector<Row>& append_rows) {
    if (append_rows.empty()) {
        return;
    }
    if (!_inited) {
        _init_mem_table();
    }
    std::unique_ptr<vectorized::MutableBlock> input_block =
            std::make_unique<vectorized::MutableBlock>(std::move(_schema->create_block()));
    assert(input_block->columns() == append_rows[0].columns.size() + 2); // 2 means vin + timestamp

    for (const auto& row : append_rows) {
        input_block->add_row(row);
    }

    _write(std::move(input_block->to_block()));
    input_block.reset();
}

void TableWriter::close() {
    if (_mem_table != nullptr) {
        _flush();
    }
}

bool TableWriter::_need_to_flush() {
    return _mem_table->rows() >= _MEM_TABLE_FLUSH_THRESHOLD;
}

void TableWriter::_write(const vectorized::Block&& block) {
    _mem_table->insert(std::move(block));
    if (_need_to_flush()) {
        _flush();
    }
}

void TableWriter::_flush() {
    size_t num_rows_written_in_table = 0;
    _mem_table->flush(&num_rows_written_in_table);
    if (num_rows_written_in_table == 0) {
        return;
    }
    _mem_table->finalize();
    _mem_table.reset();
    _file_writer->finalize();
    _file_writer->close();
    _inited = false;
    // INFO_LOG("segment_%zu has been flushed into disk, path is %s", _next_segment_id.load() - 1, _fs->root_path().c_str())
}

void TableWriter::_init_mem_table() {
    size_t segment_id = _next_segment_id++;
    io::Path segment_path = _fs->root_path() / io::Path("segment_" + std::to_string(segment_id) + ".dat");
    if (_fs->exists(segment_path)) {
        _fs->delete_file(segment_path);
    }
    _file_writer = _fs->create_file(segment_path);
    _mem_table = std::make_unique<MemTable>(_file_writer.get(), _schema, segment_id);
    _inited = true;
}

}