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

#include "storage/tsm_file.h"

namespace LindormContest {

    TsmFile::ColumnIterator TsmFile::column_begin(const std::string &column_name) const {
        size_t column_idx = 0;
        for (auto it = _index_blocks.cbegin(); it != _index_blocks.cend(); ++it) {
            if (it->_index_meta._column_name == column_name) {
                break;
            }
            column_idx++;
        }
        assert(_data_blocks.size() % SCHEMA_COLUMN_NUMS == 0);
        size_t block_per_column = _data_blocks.size() / SCHEMA_COLUMN_NUMS;
        auto block_iter = _data_blocks.cbegin() + (column_idx * block_per_column);
        auto value_iter = block_iter->_column_values.cbegin();
        return ColumnIterator(block_iter, value_iter);
    }

    TsmFile::ColumnIterator TsmFile::column_end(const std::string &column_name) const {
        size_t column_idx = 0;
        for (auto it = _index_blocks.cbegin(); it != _index_blocks.cend(); ++it) {
            if (it->_index_meta._column_name == column_name) {
                break;
            }
            column_idx++;
        }
        assert(_data_blocks.size() % SCHEMA_COLUMN_NUMS == 0);
        size_t block_per_column = _data_blocks.size() / SCHEMA_COLUMN_NUMS;
        auto block_iter = _data_blocks.cbegin() + ((column_idx + 1) * block_per_column - 1);
        auto value_iter = block_iter->_column_values.cend();
        return ColumnIterator(block_iter, value_iter);
    }

    // _offset & _size & _index_offset & _footer_offset are lazily inited
    void TsmFile::encode_to(std::string *buf) {
        size_t index_entry_count = 0;
        // encode data blocks and record offset & size
        for (auto &index_block: _index_blocks) {
            for (auto &index_entry: index_block._index_entries) {
                index_entry._offset = buf->size();
                _data_blocks[index_entry_count++].encode_to(buf);
                index_entry._size = buf->size() - index_entry._offset;
            }
        }
        assert(index_entry_count == _data_blocks.size());
        _footer._index_offset = buf->size();
        // encode index blocks
        for (const auto &block: _index_blocks) {
            block.encode_to(buf);
        }
        _footer._footer_offset = buf->size();
        _footer.encode_to(buf);
    }

    void TsmFile::decode_from(const uint8_t *buf, uint32_t file_size) {
        const uint8_t *index_offset_p = buf + file_size - 2 * sizeof(uint32_t);
        const uint8_t *footer_offset_p = buf + file_size - sizeof(uint32_t);
        uint32_t index_offset = decode_fixed<uint32_t>(index_offset_p);
        uint32_t footer_offset = decode_fixed<uint32_t>(footer_offset_p);
        // parse footer
        const uint8_t *footer_p = buf + footer_offset;
        assert((file_size - footer_offset - 2 * sizeof(uint32_t)) % sizeof(int64_t) == 0);
        size_t ts_count = (file_size - footer_offset - 2 * sizeof(uint32_t)) / sizeof(int64_t);
        _footer.decode_from(footer_p, ts_count);
        // parse index blocks
        const uint8_t *index_blocks_p = buf + index_offset;

        for (size_t i = 0; i < SCHEMA_COLUMN_NUMS; ++i) {
            IndexBlock index_block;
            index_block.decode_from(index_blocks_p);
            _index_blocks.emplace_back(std::move(index_block));
        }

        assert(index_blocks_p == buf + footer_offset);
        // parse data blocks
        const uint8_t *data_blocks_p = buf;

        for (const auto &index_block: _index_blocks) {
            for (const auto &index_entry: index_block._index_entries) {
                assert(data_blocks_p == buf + index_entry._offset);
                DataBlock data_block;
                data_block.decode_from(data_blocks_p, index_block._index_meta._type, index_entry._count);
                _data_blocks.emplace_back(std::move(data_block));
            }
        }
    }

    void TsmFile::write_to_file(const Path &tsm_file_path) {
        std::string buf;
        // encode tsm file
        encode_to(&buf);
        // flush into tsm file
        io::mmap_write_string_to_file(tsm_file_path, buf);
    }

    void TsmFile::read_from_file(const Path &tsm_file_path) {
        std::string buf;
        // read tsm content from file
        io::mmap_read_string_from_file(tsm_file_path, buf);
        // decode tsm file
        decode_from(reinterpret_cast<const uint8_t *>(buf.c_str()), buf.size());
    }
}