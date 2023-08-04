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

#include <optional>

#include "Root.h"
#include "struct/Requests.h"
#include "vec/blocks/block.h"
#include "memtable.h"
#include "table_schema.h"
#include "io/io_utils.h"
#include "io/file_system.h"
#include "io/file_writer.h"

namespace LindormContest::storage {

static constexpr size_t MEM_TABLE_FLUSH_THRESHOLD = 10000;

class TableWriter {
public:
    TableWriter(io::FileSystemSPtr fs, io::Path table_path, TableSchemaSPtr schema);

    ~TableWriter();

    void append(const WriteRequest& w_req);

    void write(const vectorized::Block&& block);

    void flush();

    bool need_to_flush();

private:
    void _init_mem_table();

    io::FileSystemSPtr _fs;
    io::Path _table_path;
    io::FileWriterPtr _file_writer;
    TableSchemaSPtr _schema;
    std::unique_ptr<MemTable> _mem_table;
    std::atomic<size_t> _next_segment_id = 0;
};

}