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

#include <ranges>

#include "Root.h"
#include "common/coding.h"
#include "storage/memmap.h"
#include "compression/compressor.h"

namespace LindormContest {

    const uint8_t INDEX_ENTRY_SIZE = 28; // 8 + 8 + 8 + 4
    const size_t DATA_BLOCK_ITEM_NUMS = 1024; // the size of one block is around 20KB

    struct IndexEntry {
        int64_t _min_time; // inclusive
        int64_t _max_time; // exclusive
        uint64_t _offset;
        uint32_t _size;

        IndexEntry() = default;

        IndexEntry(int64_t min_time, int64_t max_time, uint64_t offset, uint32_t size)
        : _min_time(min_time), _max_time(max_time), _offset(offset), _size(size) {}

        ~IndexEntry() = default;

        void encode_to(std::string* buf) const {
            put_fixed(buf, _min_time);
            put_fixed(buf, _max_time);
            put_fixed(buf, _offset);
            put_fixed(buf, _size);
        }

        void decode_from(const uint8_t*& buf) {
            _min_time = decode_fixed<int64_t>(buf);
            _max_time = decode_fixed<int64_t>(buf);
            _offset = decode_fixed<uint64_t>(buf);
            _size = decode_fixed<uint32_t>(buf);
        }

        bool contains(int64_t ts) const {
            return ts >= _min_time && ts < _max_time;
        }
    };

    struct IndexBlockMeta {
        uint32_t _count;
        ColumnType _type;
        InternalKey _key;

        IndexBlockMeta(const InternalKey& key, ColumnType type) : _key(key), _type(type), _count(0) {}

        ~IndexBlockMeta() = default;

        void encode_to(std::string* buf) const {
            put_fixed(buf, _count);
            put_fixed(buf, (uint8_t) _type);
            _key.encode_to(buf);
        }

        void decode_from(const uint8_t*& buf) {
            _count = decode_fixed<uint32_t>(buf);
            _type = (ColumnType) decode_fixed<uint8_t>(buf);
            _key.decode_from(buf);
        }
    };

    struct IndexBlock {
        IndexBlockMeta _index_meta;
        std::vector<IndexEntry> _index_entries;

        IndexBlock(const InternalKey& key, ColumnType type) : _index_meta(key, type) {}

        ~IndexBlock() = default;

        void add_entry(const IndexEntry& entry) {
            _index_entries.emplace_back(entry);
            _index_meta._count++;
        }

        void add_entry(int64_t min_time, int64_t max_time, uint64_t offset, uint32_t size) {
            _index_entries.emplace_back(min_time, max_time, offset, size);
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
            for (uint32_t i = 0; i < _index_meta._count; ++i) {
                IndexEntry index_entry;
                index_entry.decode_from(buf);
                _index_entries.emplace_back(index_entry);
            }
        }
    };

    struct DataBlock {
        uint32_t _count;
        ColumnType _type;
        std::vector<int64_t> _tss;
        std::vector<ColumnValue> _column_values;

        DataBlock(ColumnType type, uint32_t count) : _type(type), _count(count) {}

        void encode_to(std::string* buf) const {
            put_fixed(buf, _count);
            put_fixed(buf, (uint8_t) _type);
            std::string uncompress_ts_data;
            std::string uncompress_val_data;
            uncompress_ts_data.append(reinterpret_cast<const char*>(_tss.data()), _count * sizeof(int64_t));

            for (const auto &val: _column_values) {
                uncompress_val_data.append(val.columnData, val.getRawDataSize());
            }

            uint32_t uncompress_ts_size = uncompress_ts_data.size();
            uint32_t uncompress_val_size = uncompress_val_data.size();

            std::unique_ptr<char[]> compress_ts_data = std::make_unique<char[]>(uncompress_ts_size * 1.2);
            std::unique_ptr<char[]> compress_val_data = std::make_unique<char[]>(uncompress_val_size * 1.2);

            uint32_t compress_ts_size = compression::compress_int64(uncompress_ts_data.c_str(),uncompress_ts_size, compress_ts_data.get());
            uint32_t compress_val_size;

            switch (_type) {
                case COLUMN_TYPE_INTEGER:
                    compress_val_size = compression::compress_int32(uncompress_val_data.c_str(), uncompress_val_size, compress_val_data.get());
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    compress_val_size = compression::compress_float(uncompress_val_data.c_str(), uncompress_val_size, compress_val_data.get());
                    break;
                case COLUMN_TYPE_STRING:
                    compress_val_size = compression::compress_string(uncompress_val_data.c_str(), uncompress_val_size, compress_val_data.get());
                    break;
                default:
                    throw std::runtime_error("invalid column type");
            }

            put_fixed(buf, uncompress_ts_size);
            put_fixed(buf, compress_ts_size);
            buf->append(compress_ts_data.get(), compress_ts_size);
            put_fixed(buf, uncompress_val_size);
            put_fixed(buf, compress_val_size);
            buf->append(compress_val_data.get(), compress_val_size);
        }

        void decode_from(const uint8_t*& buf) {
            _count = decode_fixed<uint32_t>(buf);
            _type = (ColumnType) decode_fixed<uint8_t>(buf);

            // decode ts
            uint32_t uncompress_ts_size = decode_fixed<uint32_t>(buf);
            assert(uncompress_ts_size == _count * sizeof(int64_t));
            uint32_t compress_ts_size = decode_fixed<uint32_t>(buf);
            std::unique_ptr<char[]> uncompress_ts_data = std::make_unique<char[]>(uncompress_ts_size * 1.2);
            auto start_ptr = compression::decompress_int64(reinterpret_cast<const char*>(buf), compress_ts_size,
                                                           uncompress_ts_data.get(), uncompress_ts_size);
            _tss.resize(_count);
            std::memcpy(_tss.data(), start_ptr, uncompress_ts_size);
            buf += compress_ts_size;

            // decode value
            uint32_t uncompress_val_size = decode_fixed<uint32_t>(buf);
            uint32_t compress_val_size = decode_fixed<uint32_t>(buf);
            std::unique_ptr<char[]> uncompress_val_data = std::make_unique<char[]>(uncompress_val_size * 1.2);

            switch (_type) {
                case COLUMN_TYPE_INTEGER: {
                    assert(uncompress_val_size == _count * sizeof(int32_t));
                    auto* int_ptr = reinterpret_cast<int32_t*>(
                            compression::decompress_int32(reinterpret_cast<const char*>(buf), compress_val_size,
                                                          uncompress_val_data.get(), uncompress_val_size));
                    for (int32_t i = 0; i < _count; ++i) {
                        _column_values.emplace_back(int_ptr[i]);
                    }
                    break;
                }
                case COLUMN_TYPE_DOUBLE_FLOAT: {
                    assert(uncompress_val_size == _count * sizeof(double_t));
                    auto* double_ptr = reinterpret_cast<double_t*>(
                            compression::decompress_float(reinterpret_cast<const char*>(buf), compress_val_size,
                                                          uncompress_val_data.get(), uncompress_val_size));
                    for (int32_t i = 0; i < _count; ++i) {
                        _column_values.emplace_back(double_ptr[i]);
                    }
                    break;
                }
                case COLUMN_TYPE_STRING: {
                    compression::decompress_string(reinterpret_cast<const char*>(buf), compress_val_size,
                                                   uncompress_val_data.get(), uncompress_val_size);
                    size_t str_offset = 0;
                    while (str_offset != uncompress_val_size) {
                        int32_t str_length = *reinterpret_cast<int32_t *>(uncompress_val_data.get() + str_offset);
                        str_offset += sizeof(int32_t);
                        _column_values.emplace_back(uncompress_val_data.get() + str_offset, str_length);
                        str_offset += str_length;
                    }
                    assert(str_offset == uncompress_val_size);
                    break;
                }
                default:
                    throw std::runtime_error("invalid column type");
            }

            buf += compress_val_size;
        }
    };

    class MemMapWriter {
    public:
        MemMapWriter(const Path& tsm_file_path, SchemaSPtr schema, const MemMap& mem_map)
        : _tsm_file_path(tsm_file_path), _schema(schema), _mem_map(mem_map), _index_offset(0) {}

        void write();

        void write_internal_key(const InternalKey& key, const InternalValue& value);

        void write_block(const InternalKey& key, const InternalValue& value,
                         ColumnType type, size_t start, size_t end, IndexBlock& index_block);

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