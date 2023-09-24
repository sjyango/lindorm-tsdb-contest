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
                _data_blocks[index_entry_count++].encode_to_compress(index_block._index_meta._type, buf);
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
        _footer.encode_to_compress(buf);
    }

    void TsmFile::write_to_file(const Path &tsm_file_path) {
        std::string buf;
        // encode tsm file
        encode_to(&buf);
        // flush into tsm file
        io::stream_write_string_to_file(tsm_file_path, buf);
        // io::mmap_write_string_to_file(tsm_file_path, buf);
    }

    void TsmFile::get_size_and_offset(const Path& tsm_file_path, uint32_t& file_size, uint32_t& index_offset, uint32_t& footer_offset) {
        std::ifstream input_file(tsm_file_path, std::ios::binary | std::ios::ate);
        if (!input_file.is_open() || !input_file.good()) {
            throw std::runtime_error("failed to open the file");
        }
        file_size = input_file.tellg();
        input_file.seekg(-8, std::ios::end);
        char buf[8];
        input_file.read(buf, 8);
        index_offset = *reinterpret_cast<uint32_t*>(buf);
        footer_offset = *reinterpret_cast<uint32_t*>(buf + sizeof(uint32_t));
        input_file.close();
    }

    void TsmFile::get_footer(const Path& tsm_file_path, Footer& footer) {
        uint32_t index_offset, footer_offset, file_size;
        get_size_and_offset(tsm_file_path, file_size, index_offset, footer_offset);
        uint32_t footer_size = file_size - footer_offset;
        std::string buf;
        io::stream_read_string_from_file(tsm_file_path, footer_offset, footer_size, buf);
        footer.decode_from_decompress(buf.c_str());
    }
}