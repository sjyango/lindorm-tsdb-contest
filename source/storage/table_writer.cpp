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

#include <optional>

#include "storage/table_writer.h"

namespace LindormContest::storage {

TableWriter::TableWriter(io::FileSystemSPtr fs, TableSchemaSPtr schema, std::atomic<size_t>* next_segment_id, size_t MEM_TABLE_FLUSH_THRESHOLD)
        : _MEM_TABLE_FLUSH_THRESHOLD(MEM_TABLE_FLUSH_THRESHOLD), _fs(fs), _schema(schema), _next_segment_id(next_segment_id) {}

TableWriter::~TableWriter() = default;

void TableWriter::append(const std::vector<Row>& append_rows, bool* flushed) {
    if (append_rows.empty()) {
        return;
    }
    std::unique_ptr<vectorized::MutableBlock> input_block =
            std::make_unique<vectorized::MutableBlock>(_schema->create_block());
    assert(input_block->columns() == append_rows[0].columns.size() + 2); // 2 means vin + timestamp

    for (const auto& row : append_rows) {
        input_block->add_row(row);
        // INFO_LOG("########################################")
        // INFO_LOG("vin: %s, timestamp: %ld", std::string(row.vin.vin, 17).c_str(), row.timestamp)
        // for (const auto& item : row.columns) {
        //     switch (item.second.columnType) {
        //     case COLUMN_TYPE_STRING: {
        //         std::pair<int32_t, const char *> lengthStrPair;
        //         item.second.getStringValue(lengthStrPair);
        //         INFO_LOG("name: %s, value: %s", item.first.c_str(), lengthStrPair.second)
        //         break;
        //     }
        //     case COLUMN_TYPE_INTEGER: {
        //         int32_t val;
        //         item.second.getIntegerValue(val);
        //         INFO_LOG("name: %s, value: %d", item.first.c_str(), val)
        //         break;
        //     }
        //     case COLUMN_TYPE_DOUBLE_FLOAT: {
        //         double val;
        //         item.second.getDoubleFloatValue(val);
        //         INFO_LOG("name: %s, value: %f", item.first.c_str(), val)
        //         break;
        //     }
        //     default: {}
        //     }
        //
        // }
        // INFO_LOG("########################################")
    }

    vectorized::Block block = input_block->to_block();
    _write(&block, flushed);
}

void TableWriter::close() {
    flush();
}

bool TableWriter::_need_to_flush() {
    if (_mem_table == nullptr) {
        return false;
    }
    return _mem_table->rows() >= _MEM_TABLE_FLUSH_THRESHOLD;
}

void TableWriter::_write(const vectorized::Block* block, bool* flushed) {
    {
        // std::lock_guard<std::mutex> l(_latch);
        if (_mem_table == nullptr) {
            _init_mem_table();
        }
        _mem_table->insert(block);
        *flushed = false;
    }
    if (_need_to_flush()) {
        flush();
        *flushed = true;
    }
}

void TableWriter::flush() {
    // std::lock_guard<std::mutex> l(_latch);
    if (_mem_table == nullptr || _mem_table->rows() == 0) {
        return;
    }
    size_t num_rows_written_in_table = 0;
    _mem_table->flush(&num_rows_written_in_table);
    INFO_LOG("segment_%zu has been flushed %zu rows into disk, path is %s", _next_segment_id->load() - 1, num_rows_written_in_table, _fs->root_path().c_str())
    if (num_rows_written_in_table == 0) {
        return;
    }
    _mem_table->finalize();
    _mem_table.reset();
    _file_writer->finalize();
    _file_writer->close();
    _file_writer.reset();
}

size_t TableWriter::rows() const {
    if (_mem_table == nullptr) {
        return 0;
    }
    return _mem_table->rows();
}

void TableWriter::_init_mem_table() {
    size_t segment_id = _next_segment_id->load();
    _next_segment_id->fetch_add(1);
    io::Path segment_path = _fs->root_path() / io::Path("segment_" + std::to_string(segment_id) + ".dat");
    if (_fs->exists(segment_path)) {
        _fs->delete_file(segment_path);
    }
    _file_writer = _fs->create_file(segment_path);
    _mem_table = std::make_unique<MemTable>(_file_writer.get(), _schema, segment_id);
    INFO_LOG("segment_%zu file has been created, path is %s", segment_id, segment_path.c_str())
}

}