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

#include "storage/rowset_writer.h"

namespace LindormContest::storage {

RowsetWriter::RowsetWriter(const LindormContest::storage::TableSchema* schema) : _schema(schema) {

}

RowsetWriter::~RowsetWriter() {

}

Status RowsetWriter::add_block(const vectorized::Block* block) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    if (UNLIKELY(_segment_writer == nullptr)) {
        FlushContext ctx;
        ctx.block = block;
        RETURN_IF_ERROR(_create_segment_writer(&_segment_writer, &ctx));
    }
    return _add_block(block, &_segment_writer);
}

}