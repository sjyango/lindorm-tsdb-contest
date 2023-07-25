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

#include "Root.h"
#include "common/status.h"
#include "struct/Requests.h"
#include "vec/blocks/block.h"
#include "memtable.h"
#include "table_schema.h"

namespace LindormContest::storage {

class DeltaWriter {
public:
    static std::unique_ptr<DeltaWriter> open(const WriteRequest& w_req, const Schema& schema);

    DeltaWriter(const WriteRequest& w_req, const Schema& schema);

    ~DeltaWriter();

    Status write(const vectorized::Block* block, const std::vector<int>& row_idxs);

    Status append(const vectorized::Block* block);

    Status close();

    Status flush_mem_table();

private:
    static constexpr size_t MEM_TABLE_FLUSH_THRESHOLD = 100000;
    String _table_name;
    std::unique_ptr<TableSchema> _schema;
    std::unique_ptr<RowsetWriter> _rowset_writer;
    std::unique_ptr<MemTable> _mem_table;
    std::unique_ptr<vectorized::MutableBlock> _input_block;

};

}