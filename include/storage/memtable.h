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
#include "struct/Schema.h"
#include "skiplist.h"
#include "table_schema.h"
#include "vec/blocks/block.h"
#include "vec/blocks/mutable_block.h"
#include "segment_writer.h"

namespace LindormContest::storage {

// row pos in _input_mutable_block
struct RowInBlock {
    size_t _row_pos;
    RowInBlock(size_t row) : _row_pos(row) {}
};

class RowInBlockComparator {
public:
    RowInBlockComparator(const TableSchema* schema) : _schema(schema) {}

    // call set_block before operator().
    // only first time insert block to create _input_mutable_block,
    // so can not Comparator of construct to set _cmp_block
    void set_block(vectorized::MutableBlock* block) {
        _block = block;
    }

    int operator()(const RowInBlock* left, const RowInBlock* right) const {
        return _block->compare_at(left->_row_pos, right->_row_pos, _schema->num_key_columns(),
                                   *_block);
    }

private:
    const TableSchema* _schema;
    vectorized::MutableBlock* _block; // corresponds to MemTable::_input_mutable_block
};

class MemTable {
public:
    using VecTable = SkipList<RowInBlock*, RowInBlockComparator>;

    MemTable(const TableSchema* schema);

    ~MemTable();

    void insert(const vectorized::Block&& input_block, const std::vector<int>& row_idxs);

    bool need_to_flush(size_t threshold) const;

    /// Flush
    vectorized::Block flush();

    vectorized::Block close() {
        return flush();
    }

    /// The iterator of memtable, so that the data in this memtable
    /// can be visited outside.
    class Iterator {
    public:
        Iterator(MemTable* mem_table)
                : _mem_table(mem_table), _it(mem_table->_skip_list.get()) {}

        ~Iterator() = default;

        void seek_to_first() {
            _it.seek_to_first();
        }

        bool valid() {
            return _it.valid();
        }

        void next() {
            _it.next();
        }

    private:
        MemTable* _mem_table;
        VecTable::Iterator _it;
    };

private:
    void _collect_skip_list();

    const TableSchema* _schema;
    std::unique_ptr<RowInBlockComparator> _row_comparator;
    std::unique_ptr<Arena> _arena;
    std::unique_ptr<VecTable> _skip_list;
    VecTable::Hint _hint;
    std::vector<std::unique_ptr<RowInBlock>> _row_in_blocks;
    int64_t _rows = 0;
    vectorized::MutableBlock _input_mutable_block;
    vectorized::MutableBlock _output_mutable_block;
}; // class MemTable

}