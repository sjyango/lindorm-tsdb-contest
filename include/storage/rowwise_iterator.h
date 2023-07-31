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

#include <list>
#include <queue>

#include "Root.h"
#include "storage/partial_schema.h"
#include "vec/columns/ColumnFactory.h"

namespace LindormContest::vectorized {
class Block;
}

namespace LindormContest::storage {

struct RowLocation {
    RowLocation() : _segment_id(0), _row_id(0) {}
    RowLocation(uint32_t segment_id, uint32_t row_id) : _segment_id(segment_id), _row_id(row_id) {}

    uint32_t _segment_id;
    uint32_t _row_id;

    bool operator==(const RowLocation& rhs) const {
        return _segment_id == rhs._segment_id && _row_id == rhs._row_id;
    }

    bool operator<(const RowLocation& rhs) const {
        if (_segment_id != rhs._segment_id) {
            return _segment_id < rhs._segment_id;
        } else {
            return _row_id < rhs._row_id;
        }
    }
};

class RowwiseIterator;

using RowwiseIteratorUPtr = std::unique_ptr<RowwiseIterator>;

class RowwiseIterator {
public:
    RowwiseIterator() = default;

    virtual ~RowwiseIterator() = default;

    // Initialize this iterator and make it ready to read with
    // input options.
    // Input options may contain scan range in which this scan.
    // Return void::OK() if init successfully,
    // Return other error otherwise
    virtual void init() {
        throw std::runtime_error("next_batch to be implemented");
    }

    // If there is any valid data, this function will load data
    // into input batch with void::OK() returned
    // If there is no data to read, will return void::EndOfFile.
    // If other error happens, other error code will be returned.
    virtual void next_batch(vectorized::Block* block) {
        throw std::runtime_error("next_batch to be implemented");
    }

    virtual void next_block_view(vectorized::BlockView* block_view) {
        throw std::runtime_error("next_block_view to be implemented");
    }

    virtual void next_row(vectorized::IteratorRowRef* ref) {
        throw std::runtime_error("next_row to be implemented");
    }

    // return schema for this Iterator
    virtual PartialSchemaSPtr schema() const = 0;

    // return rows merged count by iterator
    virtual uint64_t merged_rows() const { return 0; }

    // return if it's an empty iterator
    virtual bool empty() const { return false; }
};

class MergeIteratorContext {
public:
    MergeIteratorContext(RowwiseIteratorUPtr&& iter, uint32_t order)
            : _iter(std::move(iter)),
              _order(order) {}

    MergeIteratorContext(const MergeIteratorContext&) = delete;

    MergeIteratorContext(MergeIteratorContext&&) = delete;

    MergeIteratorContext& operator=(const MergeIteratorContext&) = delete;

    MergeIteratorContext& operator=(MergeIteratorContext&&) = delete;

    ~MergeIteratorContext() = default;

    void block_reset(std::shared_ptr<vectorized::Block>& block) {
        if (block == nullptr) {
            block = std::make_shared<vectorized::Block>();
            PartialSchemaSPtr schema = _iter->schema();
            const auto& column_ids = schema->column_ids();
            for (const auto& table_column : schema->columns()) {
                vectorized::MutableColumnSPtr column = vectorized::ColumnFactory::instance().create_column(
                        table_column.get_column_type(), table_column.get_name());
                // column->reserve(_block_row_max);
                vectorized::ColumnWithTypeAndName column_with_type_and_name {
                        std::move(column), table_column.get_column_type(), table_column.get_name()};
                block->insert(column_with_type_and_name);
            }
        } else {
            block->clear_column_data();
        }
    }

    // Initialize this context and will prepare data for current_row()
    void init() {
        _fetch_block();
        advance();
    }

    bool compare(const MergeIteratorContext& rhs) const {
        assert(_iter->schema()->num_key_columns() == rhs._iter->schema()->num_key_columns());
        int res = _block->compare_at(_index_in_block, rhs._index_in_block,
                                     _iter->schema()->num_key_columns(), *rhs._block);
        if (res != 0) {
            return res > 0;
        }
        auto result = _order < rhs.order();
        result ? set_is_same(true) : rhs.set_is_same(true);
        return result;
    }

    // `advanced = false` when current block finished
    void copy_rows(vectorized::Block* block, bool advanced = true) {
        vectorized::Block& src = *_block;
        vectorized::Block& dst = *block;
        if (_cur_batch_num == 0) {
            return;
        }
        // copy a row to dst block column by column
        size_t start = _index_in_block - _cur_batch_num + 1 - advanced;

        for (size_t i = 0; i < _iter->schema()->num_columns(); ++i) {
            auto& s_col = src.get_by_position(i);
            auto& d_col = dst.get_by_position(i);
            vectorized::ColumnSPtr s_cp = s_col._column;
            vectorized::MutableColumnSPtr d_cp = vectorized::IColumn::assume_mutable(d_col._column);
            d_cp->insert_range_from(*s_cp, start, _cur_batch_num);
        }

        _cur_batch_num = 0;
    }

    void copy_rows(vectorized::Block* block, size_t count) {
        vectorized::Block& src = *_block;
        vectorized::Block& dst = *block;
        assert(count > 0);

        auto start = _index_in_block;
        _index_in_block += count - 1;

        for (size_t i = 0; i < _iter->schema()->num_columns(); ++i) {
            auto& s_col = src.get_by_position(i);
            auto& d_col = dst.get_by_position(i);

            vectorized::ColumnSPtr s_cp = s_col._column;
            vectorized::MutableColumnSPtr d_cp = vectorized::IColumn::assume_mutable(d_col._column);
            d_cp->insert_range_from(*s_cp, start, count);
        }
    }

    RowLocation current_row_location() {
        return _block_row_locations[_index_in_block];
    }

    // Advance internal row index to next valid row
    void advance() {
        _is_same = false;
        _index_in_block++;
        if (_index_in_block >= _block->rows()) {
            _valid = false;
        }
    }

    bool is_valid() const {
        return _valid;
    }

    uint32_t order() const {
        return _order;
    }

    void set_is_same(bool is_same) const {
        _is_same = is_same;
    }

    bool is_same() const {
        return _is_same;
    }

    void add_cur_batch() {
        _cur_batch_num++;
    }

    bool is_cur_block_finished() {
        return _index_in_block == _block->rows() - 1;
    }

    size_t remain_rows() {
        return _block->rows() - _index_in_block;
    }

    bool is_first_row() const {
        return _is_first_row;
    }

    void set_is_first_row(bool is_first_row) {
        _is_first_row = is_first_row;
    }

    void set_cur_row_ref(vectorized::IteratorRowRef* ref) {
        ref->block = _block;
        ref->row_pos = _index_in_block;
    }

private:
    // Load next block into _block
    void _fetch_block() {
        block_reset(_block);
        _iter->next_batch(_block.get());
        _index_in_block = -1;
        _valid = true;
    }

    RowwiseIteratorUPtr _iter;
    // segment order, used to compare key
    uint32_t _order = 0;
    bool _valid = false;
    mutable bool _is_same = false;
    int32_t _index_in_block = -1;
    size_t _cur_batch_num = 0;

    std::shared_ptr<vectorized::Block> _block;
    // use to identify whether it's first block load from RowwiseIterator
    bool _is_first_row = true;
    std::vector<RowLocation> _block_row_locations;
};

// Row source represent row location in multi-segments
// use a uint16_t to store info
// the lower 15 bits means segment_id in segment pool, and the higher 1 bits means agg flag.
// In unique-key, agg flags means this key should be deleted, this comes from two way: old version
// key or delete_sign.
class RowSource {
public:
    RowSource(uint16_t data) : _data(data) {}

    RowSource(uint16_t source_num, bool del_flag) {
        _data = (source_num & SOURCE_FLAG) | (source_num & DEL_FLAG);
        _data = del_flag ? (_data | DEL_FLAG) : (_data & SOURCE_FLAG);
    }

    uint16_t get_source_num() const {
        return _data & SOURCE_FLAG;
    }

    bool del_flag() const {
        return (_data & DEL_FLAG) != 0;
    }

    void set_del_flag(bool del_flag) {
        _data = del_flag ? (_data | DEL_FLAG) : (_data & SOURCE_FLAG);
    }

    uint16_t data() const {
        return _data;
    }

private:
    uint16_t _data;
    static const uint16_t SOURCE_FLAG = 0x7FFF;
    static const uint16_t DEL_FLAG = 0x8000;
};

/* rows source buffer
this buffer should have a memory limit, once reach memory limit, write
buffer data to tmp file.
usage:
    RowSourcesBuffer buffer(tablet_id, tablet_storage_path, reader_type);
    buffer.append()
    buffer.append()
    buffer.flush()
    buffer.seek_to_begin()
    while (buffer.has_remaining().ok()) {
        auto cur = buffer.current().get_source_num();
        auto same = buffer.same_source_count(cur, limit);
        // do copy block data
        buffer.advance(same);
    }
*/
class RowSourcesBuffer {
public:
    RowSourcesBuffer() = default;
    
    ~RowSourcesBuffer() = default;

    // write batch row source
    void append(const std::vector<RowSource>& row_sources) {
        for (const auto& source : row_sources) {
            _buffer.push_back(source.data());
        }
        _total_size += row_sources.size();
    }

    RowSource current() {
        assert(_buf_idx < _buffer.size());
        return RowSource(_buffer[_buf_idx]);
    }
    
    void advance(int32_t step = 1) {
        assert(_buf_idx + step <= _buffer.size());
        _buf_idx += step;
    }

    uint64_t buf_idx() const { 
        return _buf_idx;
    }
    
    uint64_t total_size() const {
        return _total_size;
    }
    
    uint64_t buffered_size() {
        return _buffer.size();
    }
    
    void set_del_flag(uint64_t index, bool del) {
        assert(index < _buffer.size());
        RowSource row_source(_buffer[index]);
        row_source.set_del_flag(del);
        _buffer[index] = row_source.data();
    }

    bool has_remaining() {
        return _buf_idx < _buffer.size();
    }

    void seek_to_begin() {
        _buf_idx = 0;
    }

    size_t same_source_count(uint16_t source, size_t limit) {
        int result = 1;
        int start = _buf_idx + 1;
        int end = _buffer.size();
        
        while (result < limit && start < end) {
            RowSource next(_buffer[start++]);
            if (source != next.get_source_num()) {
                break;
            }
            ++result;
        }
        
        return result;
    }

    // return continous del_flag=true count from index
    size_t continuous_agg_count(uint64_t index) {
        size_t result = 1;
        int start = index + 1;
        int end = _buffer.size();
        
        while (start < end) {
            RowSource next(_buffer[start++]);
            if (next.del_flag()) {
                ++result;
            } else {
                break;
            }
        }
        
        return result;
    }

private:
    uint64_t _buf_idx = 0;
    std::vector<uint16_t> _buffer;
    uint64_t _total_size = 0;
};

class MergeIterator : public RowwiseIterator {
public:
    MergeIterator(std::vector<RowwiseIteratorUPtr>&& segment_iters, PartialSchemaSPtr schema, RowSourcesBuffer* row_sources_buffer)
            : _segment_iters(std::move(segment_iters)),
              _schema(schema),
              _row_sources_buffer(row_sources_buffer) {}

    ~MergeIterator() override {
        while (!_merge_heap.empty()) {
            auto ctx = _merge_heap.top();
            _merge_heap.pop();
            delete ctx;
        }
    }

    void init() override {
        if (_segment_iters.empty()) {
            return;
        }
        uint32_t seg_order = 0;

        for (auto& iter : _segment_iters) {
            MergeIteratorContext* ctx = new MergeIteratorContext(std::move(iter), seg_order);
            _segment_iter_ctx.push_back(ctx);
            ctx->init();
            if (!ctx->is_valid()) {
                ++seg_order;
                delete ctx;
                continue;
            }
            _merge_heap.push(ctx);
            ++seg_order;
        }

        _segment_iters.clear();
    }

    void next_batch(vectorized::Block* block) override {
        size_t row_idx = 0;
        MergeIteratorContext* pre_ctx = nullptr;
        std::vector<RowSource> tmp_row_sources;
        while (!_merge_heap.empty()) {
            auto ctx = _merge_heap.top();
            _merge_heap.pop();
            if (ctx->is_same()) {
                tmp_row_sources.emplace_back(ctx->order(), true);
            } else {
                tmp_row_sources.emplace_back(ctx->order(), false);
            }
            if (ctx->is_same()) {
                // skip cur row, copy pre ctx
                ++_merged_rows;
                if (pre_ctx) {
                    pre_ctx->copy_rows(block);
                    pre_ctx = nullptr;
                }
            } else {
                ctx->add_cur_batch();
                if (pre_ctx != ctx) {
                    if (pre_ctx) {
                        pre_ctx->copy_rows(block);
                    }
                    pre_ctx = ctx;
                }
                row_idx++;
                if (ctx->is_cur_block_finished()) {
                    // current block finished, ctx not advance
                    // so copy start_idx = (_index_in_block - _cur_batch_num + 1)
                    ctx->copy_rows(block, false);
                    pre_ctx = nullptr;
                }
            }
            ctx->advance();
            if (ctx->is_valid()) {
                _merge_heap.push(ctx);
            } else {
                delete ctx;
            }
        }
        _row_sources_buffer->append(tmp_row_sources);
    }

    PartialSchemaSPtr schema() const override {
        return _schema;
    }

    uint64_t merged_rows() const override {
        return _merged_rows;
    }

private:
    struct MergeContextComparator {
        bool operator()(const MergeIteratorContext* lhs, const MergeIteratorContext* rhs) const {
            return lhs->compare(*rhs);
        }
    };

    using MergeHeap = std::priority_queue<MergeIteratorContext*,
                                           std::vector<MergeIteratorContext*>,
                                           MergeContextComparator>;

    MergeHeap _merge_heap;
    PartialSchemaSPtr _schema;
    std::vector<MergeIteratorContext*> _segment_iter_ctx;
    RowSourcesBuffer* _row_sources_buffer;
    uint32_t _merged_rows = 0;
    std::vector<RowLocation> _block_row_locations;
    std::vector<RowwiseIteratorUPtr> _segment_iters;
};


}