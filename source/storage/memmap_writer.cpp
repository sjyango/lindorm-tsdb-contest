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

#include "storage/memmap_writer.h"

#include <fstream>

namespace LindormContest {

    void MemMapWriter::write() {
        // data blocks
        for (const auto & [key, value] : _mem_map.get_mem_map()) {
            write_internal_key(key, value);
        }
        _index_offset = _buf.size();
        // index blocks
        for (const auto &index_block: _index_blocks)  {
            index_block->encode_to(&_buf);
        }
        // footer
        put_fixed(&_buf, _index_offset);
        // flush buf to create tsm file
        flush();
    }

    void MemMapWriter::flush() {
        std::ofstream tsm_file(_tsm_file_path);
        if (!tsm_file.is_open() || !tsm_file.good()) {
            throw std::runtime_error("error open tsm_file");
        }
        tsm_file << _buf;
        tsm_file.flush();
        tsm_file.close();
    }

    // write a <InternalKey, InternalValue> pair in _mem_map
    // a <InternalKey, InternalValue> pair has an index block
    void MemMapWriter::write_internal_key(const InternalKey& key, const InternalValue& value) {
        ColumnType type = _schema->columnTypeMap[key._column_name];
        std::unique_ptr<IndexBlock> index_block = std::make_unique<IndexBlock>(key, type);

        for (size_t start = 0; start < value.size(); start += DATA_BLOCK_ITEM_NUMS) {
            size_t end = std::min(start + DATA_BLOCK_ITEM_NUMS, value.size());
            write_block(key, value, type, start, end, *index_block);
        }

        _index_blocks.emplace_back(std::move(index_block));
    }

    // build a block per DATA_BLOCK_ITEM_NUMS items
    // add an index entry into index block
    void MemMapWriter::write_block(const InternalKey &key, const InternalValue &value,
                                   ColumnType type, size_t start, size_t end, IndexBlock& index_block) {
        std::unique_ptr<DataBlock> data_block = std::make_unique<DataBlock>(type, end - start);
        data_block->_tss.assign(value._tss.begin() + start, value._tss.begin() + end);
        data_block->_column_values.assign(value._column_values.begin() + start, value._column_values.begin() + end);

        IndexEntry index_entry;
        index_entry._min_time = std::numeric_limits<int64_t>::max();
        index_entry._max_time = std::numeric_limits<int64_t>::min();

        for (const auto &ts: data_block->_tss) {
            index_entry._min_time = std::min(index_entry._min_time, ts);
            index_entry._max_time = std::max(index_entry._max_time, ts);
        }

        index_entry._offset = _buf.size();
        data_block->encode_to(&_buf);
        index_entry._size = _buf.size() - index_entry._offset;
        index_block.add_entry(index_entry);
    }

}