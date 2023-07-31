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

#include <algorithm>

#include "Root.h"
#include "partial_schema.h"
#include "rowwise_iterator.h"
#include "segment_reader.h"
#include "segment_traits.h"
#include "struct/Requests.h"
#include "vec/blocks/block.h"

namespace LindormContest::storage {

class TableReader {
public:
    TableReader() = default;

    ~TableReader() = default;

    void init(std::vector<SegmentSPtr>& segment_datas, const String& table_name, PartialSchemaSPtr schema, const String& key) {
        _segment_datas = segment_datas;
        _table_name = table_name;
        _schema = schema;
        std::vector<RowwiseIteratorUPtr> segment_iters;
        for (auto& segment_data : _segment_datas) {
            RowwiseIteratorUPtr segment_iter = std::make_unique<SegmentReader>(segment_data, _schema, key);
            segment_iters.emplace_back(std::move(segment_iter));
        }
        _table_iter = std::make_unique<MergeIterator>(std::move(segment_iters), _schema, nullptr);
        _inited = true;
    }

    void reset() {
        _segment_datas.clear();
        _table_name.clear();
        _schema.reset();
        _table_iter.reset();
        _inited = false;
    }

    Row execute_latest_query(const LatestQueryRequest& req) {
        vectorized::Block block = _schema->create_block();
        _next_batch(&block);
        return std::move(block.to_row(block.rows() - 1));
    }

    std::vector<Row> execute_time_range_query(const TimeRangeQueryRequest& req) {
        vectorized::Block block = _schema->create_block();
        _next_batch(&block);
        size_t start_row = _lower_bound(&block, req.timeLowerBound);
        size_t end_row = _lower_bound(&block, req.timeUpperBound);
        return std::move(block.to_rows(start_row, end_row));
    }

private:
    void _next_batch(vectorized::Block* block) {
        assert(_inited);
        _table_iter->next_batch(block);
    }

    size_t _lower_bound(const vectorized::Block* block, uint64_t timestamp) const {
        const vectorized::ColumnInt64& col = reinterpret_cast<const vectorized::ColumnInt64&>(*block->get_by_position(1)._column);
        auto it = std::lower_bound(col.get_data().cbegin(), col.get_data().cend(), timestamp);
        return std::distance(col.get_data().cbegin(), it);
    }

    bool _inited = false;
    std::vector<SegmentSPtr> _segment_datas;
    String _table_name;
    PartialSchemaSPtr _schema;
    std::unique_ptr<RowwiseIterator> _table_iter;
};

}