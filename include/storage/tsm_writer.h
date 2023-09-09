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

/*
A TSM file is composed for three sections: blocks, indexes and footer.

┌─────────────────────────────────────┬─────────────┬──────────────┐
│                Blocks               │   Indexes   │    Footer    │
│                N bytes              │   N bytes   │    8 bytes   │
└─────────────────────────────────────┴─────────────┴──────────────┘

Blocks are sequences of pairs of CRC32 and data.  The block data is opaque to the
file.  The CRC32 is used for block level error detection. The length of the blocks
is stored in the index.

┌─────────────────────────────┐
│            Blocks           │
├─────────┬─────────┬─────────┤
│ Block 1 │ Block 2 │ Block N │
├─────────┼─────────┼─────────┤
│  Data   │  Data   │  Data   │
│ N bytes │ N bytes │ N bytes │
└─────────┴─────────┴─────────┘

┌───────────────────────────────────────────────┐
│                   Data Block                  │
├──────────┬───────────┬─────────┬──────────┬───┤
│   Type   │  Len(TS)  │   TSs   │  Values  │...│
│  2 bytes │  2 bytes  │ N bytes │  N bytes │   │
└──────────┴───────────┴─────────┴──────────┴───┘

Following the blocks is the index for the blocks in the file.  The index is
composed of a sequence of index entries ordered lexicographically by key and
then by time.  Each index entry starts with a key length and key followed by a
count of the number of blocks in the file.  Each block entry is composed of
the min and max time for the block, the offset into the file where the block
is located and the size of the block.

The index structure can provide efficient access to all blocks as well as the
ability to determine the cost associated with accessing a given key.  Given a key
and timestamp, we can determine whether a file contains the block for that
timestamp as well as where that block resides and how much data to read to
retrieve the block.  If we know we need to read all or multiple blocks in a
file, we can use the size to determine how much to read in a given IO.

┌────────────────────────────────────────────────────────────────────────────┐
│                              Index Block                                   │
├─────────┬─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬───┤
│ Key Len │   Key   │ Type │ Count │Min Time │Max Time │ Offset │  Size  │...│
│ 2 bytes │ N bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │ 8 bytes│ 4 bytes│   │
└─────────┴─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴───┘

The last section is the footer that stores the offset of the start of the index.

┌─────────┐
│ Footer  │
├─────────┤
│Index Ofs│
│ 8 bytes │
└─────────┘
*/

#pragma once

#include <variant>

#include "Root.h"
#include "struct/Schema.h"
#include "common/coding.h"
#include "storage/memmap.h"
#include "compression/compressor.h"

namespace LindormContest {

    const size_t DATA_BLOCK_ITEM_NUMS = 1024; // the size of one block is around 20KB

    // one index entry corresponds to one data block
    // an index block has a batch of index entry
    struct IndexEntry {
        uint16_t _min_time_index;
        uint16_t _max_time_index;
        std::variant<int64_t, double_t> _sum;
        uint16_t _count;
        uint32_t _offset;
        uint32_t _size;

        void encode_to(std::string* buf) const {
            put_fixed(buf, _min_time_index);
            put_fixed(buf, _max_time_index);
            if (std::holds_alternative<int64_t>(_sum)) {
                int64_t int_value = std::get<int64_t>(_sum);
                put_fixed(buf, int_value);
            } else if (std::holds_alternative<double_t>(_sum)) {
                double_t double_value = std::get<double_t>(_sum);
                put_fixed(buf, double_value);
            } else {
                throw std::runtime_error("invalid variant type");
            }
            put_fixed(buf, _count);
            put_fixed(buf, _offset);
            put_fixed(buf, _size);
        }

        void decode_from(const uint8_t*& buf, ColumnType type) {
            _min_time_index = decode_fixed<uint16_t>(buf);
            _max_time_index = decode_fixed<uint16_t>(buf);
            if (type == COLUMN_TYPE_INTEGER) {
                int64_t int_value = decode_fixed<int64_t>(buf);
                _sum.emplace<int64_t>(int_value);
            } else if (type == COLUMN_TYPE_DOUBLE_FLOAT) {
                double_t double_value = decode_fixed<double_t>(buf);
                _sum.emplace<double_t>(double_value);
            } else {
                throw std::runtime_error("invalid variant type");
            }
            _count = decode_fixed<uint16_t>(buf);
            _offset = decode_fixed<uint32_t>(buf);
            _size = decode_fixed<uint32_t>(buf);
        }
    };

    // one index block meta corresponds to one index block, and corresponds to one column
    struct IndexBlockMeta {
        uint16_t _count;
        ColumnType _type;
        std::string _column_name;

        IndexBlockMeta() = default;

        IndexBlockMeta(const std::string& column_name, ColumnType type)
        : _column_name(column_name), _type(type), _count(0) {}

        IndexBlockMeta(IndexBlockMeta&& other) noexcept
        : _column_name(std::move(other._column_name)), _type(other._type), _count(other._count) {}

        ~IndexBlockMeta() = default;

        void encode_to(std::string* buf) const {
            put_fixed(buf, _count);
            put_fixed(buf, (uint8_t) _type);
            put_fixed(buf, (uint8_t) _column_name.size());
            buf->append(_column_name);
        }

        void decode_from(const uint8_t*& buf) {
            _count = decode_fixed<uint16_t>(buf);
            _type = (ColumnType) decode_fixed<uint8_t>(buf);
            uint8_t column_name_size = decode_fixed<uint8_t>(buf);
            _column_name.assign(reinterpret_cast<const char*>(buf), column_name_size);
            buf += column_name_size;
        }
    };

    // one entry corresponds to one column
    struct IndexBlock {
        IndexBlockMeta _index_meta;
        std::vector<IndexEntry> _index_entries;

        IndexBlock() = default;

        IndexBlock(const std::string& column_name, ColumnType type) : _index_meta(column_name, type) {}

        IndexBlock(IndexBlock&& other) noexcept
        : _index_meta(std::move(other._index_meta)), _index_entries(std::move(other._index_entries)) {}

        ~IndexBlock() = default;

        void add_entry(const IndexEntry& entry) {
            _index_entries.emplace_back(entry);
            _index_meta._count++;
        }

        void encode_to(std::string* buf) const {
            _index_meta.encode_to(buf);
            for (const auto &entry: _index_entries) {
                entry.encode_to(buf);
            }
        }

        void decode_from(const uint8_t*& buf) {
            _index_meta.decode_from(buf);
            for (uint16_t i = 0; i < _index_meta._count; ++i) {
                IndexEntry index_entry;
                index_entry.decode_from(buf, _index_meta._type);
                _index_entries.emplace_back(index_entry);
            }
        }
    };

    // one data block contains a part of column data
    struct DataBlock {
        std::vector<ColumnValue> _column_values;

        DataBlock() = default;

        DataBlock(DataBlock&& other) : _column_values(std::move(other._column_values)) {}

        ~DataBlock() = default;

        void encode_to(std::string* buf) const {
            for (const auto &val: _column_values) {
                buf->append(val.columnData, val.getRawDataSize());
            }
        }

        void decode_from(const uint8_t*& buf, ColumnType type, uint16_t count) {
            switch (type) {
                case COLUMN_TYPE_INTEGER: {
                    for (uint16_t i = 0; i < count; ++i) {
                        int32_t int_value = decode_fixed<int32_t>(buf);
                        _column_values.emplace_back(int_value);
                    }
                    break;
                }
                case COLUMN_TYPE_DOUBLE_FLOAT: {
                    for (uint16_t i = 0; i < count; ++i) {
                        double_t double_value = decode_fixed<double_t>(buf);
                        _column_values.emplace_back(double_value);
                    }
                    break;
                }
                case COLUMN_TYPE_STRING: {
                    for (uint16_t i = 0; i < count; ++i) {
                        int32_t str_length = decode_fixed<int32_t>(buf);
                        _column_values.emplace_back((const char*) buf, str_length);
                        buf += str_length;
                    }
                    break;
                }
                default:
                    throw std::runtime_error("invalid column type");
                }
        }
    };

    // footer contains all timestamps data
    struct Footer {
        std::vector<int64_t> _tss;
        uint32_t _index_offset;
        uint32_t _footer_offset;

        Footer() = default;

        ~Footer() = default;

        void encode_to(std::string* buf) const {
            buf->append((const char*) _tss.data(), _tss.size() * sizeof(int64_t));
            put_fixed(buf, _index_offset);
            put_fixed(buf, _footer_offset);
        }

        void decode_from(const uint8_t*& buf, size_t ts_size) {
            _tss.resize(ts_size);
            std::memcpy(_tss.data(), buf, ts_size * sizeof(int64_t));
            buf += ts_size * sizeof(int64_t);
            _index_offset = decode_fixed<uint32_t>(buf);
            _footer_offset = decode_fixed<uint32_t>(buf);
        }
    };

    struct TsmFile {
        std::vector<DataBlock> _data_blocks;
        std::vector<IndexBlock> _index_blocks;
        Footer _footer;

        TsmFile() = default;

        ~TsmFile() = default;

        void encode_to(std::string* buf) const {
            for (const auto &block: _data_blocks) {
                block.encode_to(buf);
            }
            for (const auto &block: _index_blocks) {
                block.encode_to(buf);
            }
            _footer.encode_to(buf);
        }

        void decode_from(const uint8_t* buf, uint32_t file_size) {
            const uint8_t* index_offset_p = buf + file_size - 2 * sizeof(uint32_t);
            const uint8_t* footer_offset_p = buf + file_size - sizeof(uint32_t);
            uint32_t index_offset = decode_fixed<uint32_t>(index_offset_p);
            uint32_t footer_offset = decode_fixed<uint32_t>(footer_offset_p);
            // parse footer
            const uint8_t* footer_p = buf + footer_offset;
            assert((file_size - footer_offset - 2 * sizeof(uint32_t)) % sizeof(int64_t) == 0);
            size_t ts_size = (file_size - footer_offset - 2 * sizeof(uint32_t)) / sizeof(int64_t);
            _footer.decode_from(footer_p, ts_size);
            // parse index blocks
            const uint8_t* index_blocks_p = buf + index_offset;
            for (size_t i = 0; i < SCHEMA_COLUMN_NUMS; ++i) {
                IndexBlock index_block;
                index_block.decode_from(index_blocks_p);
                _index_blocks.emplace_back(std::move(index_block));
            }
            assert(index_blocks_p == buf + footer_offset);
            // parse data blocks
            const uint8_t* data_blocks_p = buf;
            for (const auto &index_block: _index_blocks) {
                for (const auto &index_entry: index_block._index_entries) {
                    assert(data_blocks_p == buf + index_entry._offset);
                    DataBlock data_block;
                    data_block.decode_from(data_blocks_p, index_block._index_meta._type, index_entry._count);
                    _data_blocks.emplace_back(std::move(data_block));
                }
            }
        }
    };

    class TsmWriter {
    public:
        MemMapWriter(const Path& tsm_file_path, SchemaSPtr schema, const MemMap& mem_map)
        : _tsm_file_path(tsm_file_path), _schema(schema), _mem_map(mem_map), _index_offset(0) {}

        void write();

        void write_internal_key(const std::string& column_name, const InternalValue& value);

        void write_block(const InternalValue& value, ColumnType type,
                         size_t start, size_t end, IndexBlock& index_block);

        void flush();

    private:
        const Path& _tsm_file_path;
        size_t _index_offset;
        const MemMap& _mem_map;
        SchemaSPtr _schema;
        std::string _buf;
        std::vector<std::unique_ptr<IndexBlock>> _index_blocks;
    };
}


// void encode_to(std::string* buf) const {
//     std::string uncompress_ts_data;
//     std::string uncompress_val_data;
//     uncompress_ts_data.append(reinterpret_cast<const char*>(_tss.data()), _count * sizeof(int64_t));
//
//     for (const auto &val: _column_values) {
//         uncompress_val_data.append(val.columnData, val.getRawDataSize());
//     }
//
//     uint32_t uncompress_ts_size = uncompress_ts_data.size();
//     uint32_t uncompress_val_size = uncompress_val_data.size();
//
//     std::unique_ptr<char[]> compress_ts_data = std::make_unique<char[]>(uncompress_ts_size * 1.2);
//     std::unique_ptr<char[]> compress_val_data = std::make_unique<char[]>(uncompress_val_size * 1.2);
//
//     uint32_t compress_ts_size = compression::compress_int64(uncompress_ts_data.c_str(),uncompress_ts_size, compress_ts_data.get());
//     uint32_t compress_val_size;
//
//     switch (_type) {
//         case COLUMN_TYPE_INTEGER:
//             compress_val_size = compression::compress_int32(uncompress_val_data.c_str(), uncompress_val_size, compress_val_data.get());
//             break;
//         case COLUMN_TYPE_DOUBLE_FLOAT:
//             compress_val_size = compression::compress_float(uncompress_val_data.c_str(), uncompress_val_size, compress_val_data.get());
//             break;
//         case COLUMN_TYPE_STRING:
//             compress_val_size = compression::compress_string(uncompress_val_data.c_str(), uncompress_val_size, compress_val_data.get());
//             break;
//         default:
//             throw std::runtime_error("invalid column type");
//     }
//
//     put_fixed(buf, uncompress_ts_size);
//     put_fixed(buf, compress_ts_size);
//     buf->append(compress_ts_data.get(), compress_ts_size);
//     put_fixed(buf, uncompress_val_size);
//     put_fixed(buf, compress_val_size);
//     buf->append(compress_val_data.get(), compress_val_size);
// }
//
// void decode_from(const uint8_t*& buf) {
//     _count = decode_fixed<uint32_t>(buf);
//     _type = (ColumnType) decode_fixed<uint8_t>(buf);
//
//     // decode ts
//     uint32_t uncompress_ts_size = decode_fixed<uint32_t>(buf);
//     assert(uncompress_ts_size == _count * sizeof(int64_t));
//     uint32_t compress_ts_size = decode_fixed<uint32_t>(buf);
//     std::unique_ptr<char[]> uncompress_ts_data = std::make_unique<char[]>(uncompress_ts_size * 1.2);
//     auto start_ptr = compression::decompress_int64(reinterpret_cast<const char*>(buf), compress_ts_size,
//                                                    uncompress_ts_data.get(), uncompress_ts_size);
//     _tss.resize(_count);
//     std::memcpy(_tss.data(), start_ptr, uncompress_ts_size);
//     buf += compress_ts_size;
//
//     // decode value
//     uint32_t uncompress_val_size = decode_fixed<uint32_t>(buf);
//     uint32_t compress_val_size = decode_fixed<uint32_t>(buf);
//     std::unique_ptr<char[]> uncompress_val_data = std::make_unique<char[]>(uncompress_val_size * 1.2);
//
//     switch (_type) {
//         case COLUMN_TYPE_INTEGER: {
//             assert(uncompress_val_size == _count * sizeof(int32_t));
//             auto* int_ptr = reinterpret_cast<int32_t*>(
//                     compression::decompress_int32(reinterpret_cast<const char*>(buf), compress_val_size,
//                                                   uncompress_val_data.get(), uncompress_val_size));
//             for (int32_t i = 0; i < _count; ++i) {
//                 _column_values.emplace_back(int_ptr[i]);
//             }
//             break;
//         }
//         case COLUMN_TYPE_DOUBLE_FLOAT: {
//             assert(uncompress_val_size == _count * sizeof(double_t));
//             auto* double_ptr = reinterpret_cast<double_t*>(
//                     compression::decompress_float(reinterpret_cast<const char*>(buf), compress_val_size,
//                                                   uncompress_val_data.get(), uncompress_val_size));
//             for (int32_t i = 0; i < _count; ++i) {
//                 _column_values.emplace_back(double_ptr[i]);
//             }
//             break;
//         }
//         case COLUMN_TYPE_STRING: {
//             compression::decompress_string(reinterpret_cast<const char*>(buf), compress_val_size,
//                                            uncompress_val_data.get(), uncompress_val_size);
//             size_t str_offset = 0;
//             while (str_offset != uncompress_val_size) {
//                 int32_t str_length = *reinterpret_cast<int32_t *>(uncompress_val_data.get() + str_offset);
//                 str_offset += sizeof(int32_t);
//                 _column_values.emplace_back(uncompress_val_data.get() + str_offset, str_length);
//                 str_offset += str_length;
//             }
//             assert(str_offset == uncompress_val_size);
//             break;
//         }
//         default:
//             throw std::runtime_error("invalid column type");
//     }
//
//     buf += compress_val_size;
// }