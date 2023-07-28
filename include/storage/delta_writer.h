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
#include "struct/Requests.h"
#include "vec/blocks/block.h"
#include "memtable.h"
#include "table_schema.h"
#include "segment_writer.h"

namespace LindormContest::storage {

static constexpr size_t MEM_TABLE_FLUSH_THRESHOLD = 10000;

class DeltaWriter {
public:
    static std::unique_ptr<DeltaWriter> open(const String& table_name, const Schema& schema);

    DeltaWriter(const String& table_name, const Schema& schema);

    ~DeltaWriter();

    std::optional<SegmentData> append(const WriteRequest& w_req);

    std::optional<SegmentData> write(const vectorized::Block&& block);

    std::optional<SegmentData> flush();

    void close();

    void reset();

    bool need_to_flush();

    SegmentData flush_segment_writer();

    size_t allocate_segment_id() { return _next_segment_id++; };

private:
    String _table_name;
    std::unique_ptr<TableSchema> _schema;
    std::unique_ptr<SegmentWriter> _segment_writer;
    std::unique_ptr<MemTable> _mem_table;
    std::atomic<size_t> _next_segment_id = 0;
    size_t _num_rows_written_in_table = 0;
};

}