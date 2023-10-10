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

#include "struct/Schema.h"
#include "common/coding.h"
#include "common/thread_pool.h"
#include "common/time_range.h"
#include "compression/compressor.h"
#include "io/io_utils.h"

namespace LindormContest {

    struct IndexEntry {
        char _sum[8];        // int64_t or double_t
        char _max[8];        // int32_t or double_t
        uint32_t _offset;
        uint32_t _size;

        template <typename T>
        T get_sum() const {
            return *reinterpret_cast<const T*>(_sum);
        }

        template <typename T>
        void set_sum(T sum) {
            *reinterpret_cast<T*>(_sum) = sum;
        }

        template <typename T>
        T get_max() const {
            return *reinterpret_cast<const T*>(_max);
        }

        template <typename T>
        void set_max(T max) {
            *reinterpret_cast<T*>(_max) = max;
        }

        void encode_to(std::string *buf) const {
            buf->append(_sum, 8);
            buf->append(_max, 8);
            put_fixed(buf, _offset);
            put_fixed(buf, _size);
        }

        void decode_from(const uint8_t *&buf) {
            std::memcpy(_sum, buf, 8);
            buf += 8;
            std::memcpy(_max, buf, 8);
            buf += 8;
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

        IndexBlockMeta(const std::string &column_name, ColumnType type)
                : _column_name(column_name), _type(type), _count(0) {}

        IndexBlockMeta(const IndexBlockMeta& other) = default;

        IndexBlockMeta& operator=(const IndexBlockMeta& other) = default;

        IndexBlockMeta(IndexBlockMeta &&other) noexcept
                : _column_name(std::move(other._column_name)), _type(other._type), _count(other._count) {}

        ~IndexBlockMeta() = default;

        void encode_to(std::string *buf) const {
            put_fixed(buf, _count);
            put_fixed(buf, (uint8_t) _type);
            put_fixed(buf, (uint8_t) _column_name.size());
            buf->append(_column_name);
        }

        void decode_from(const uint8_t *&buf) {
            _count = decode_fixed<uint16_t>(buf);
            _type = (ColumnType) decode_fixed<uint8_t>(buf);
            uint8_t column_name_size = decode_fixed<uint8_t>(buf);
            _column_name.assign(reinterpret_cast<const char *>(buf), column_name_size);
            buf += column_name_size;
        }
    };

    // one entry corresponds to one column
    struct IndexBlock {
        IndexBlockMeta _index_meta;
        std::vector<IndexEntry> _index_entries;

        IndexBlock() = default;

        IndexBlock(const std::string &column_name, ColumnType type) : _index_meta(column_name, type) {}

        IndexBlock(const IndexBlock& other) = default;

        IndexBlock& operator=(const IndexBlock& other) = default;

        IndexBlock(IndexBlock &&other) noexcept
                : _index_meta(std::move(other._index_meta)), _index_entries(std::move(other._index_entries)) {}

        ~IndexBlock() = default;

        void add_entry(const IndexEntry &entry) {
            _index_entries.emplace_back(entry);
            _index_meta._count++;
        }

        void get_index_entries_and_ranges(const TimeRange& file_tr, std::vector<IndexEntry>& index_entries, std::vector<IndexRange>& ranges) {
            for (uint16_t i = file_tr._start_idx / DATA_BLOCK_ITEM_NUMS; i <= file_tr._end_idx / DATA_BLOCK_ITEM_NUMS; ++i) {
                index_entries.emplace_back(_index_entries[i]);
                ranges.emplace_back(std::max(file_tr._start_idx, (uint16_t) (i * DATA_BLOCK_ITEM_NUMS)) % DATA_BLOCK_ITEM_NUMS,
                                    std::min(file_tr._end_idx, (uint16_t) ((i + 1) * DATA_BLOCK_ITEM_NUMS - 1)) % DATA_BLOCK_ITEM_NUMS);
            }
        }

        void encode_to(std::string *buf) const {
            _index_meta.encode_to(buf);
            for (const auto &entry: _index_entries) {
                entry.encode_to(buf);
            }
        }

        void decode_from(const uint8_t *&buf) {
            _index_meta.decode_from(buf);
            for (uint16_t i = 0; i < _index_meta._count; ++i) {
                IndexEntry index_entry;
                index_entry.decode_from(buf);
                _index_entries.emplace_back(index_entry);
            }
        }
    };

    enum class CompressCodec : uint8_t {
        SAME,
        RLE,
        SIMPLE8B,
        PLAIN
    };

    struct DataBlock {
        DataBlock() = default;

        virtual ~DataBlock() = default;

        virtual void encode_to_compress(std::string *buf) const = 0;

        virtual void decode_from_decompress(const char* buf) = 0;
    };

    struct IntDataBlock : public DataBlock {
        std::array<int32_t, DATA_BLOCK_ITEM_NUMS> _column_values;
        bool _is_same = false;

        IntDataBlock() = default;

        ~IntDataBlock() override = default;

        void encode_to_compress(std::string *buf) const override {
            if (_is_same) {
                encode_to_same(buf);
                return;
            }
            if (encode_to_simple8b(buf)) {
                return;
            }
            encode_to_plain(buf);
        }

        void decode_from_decompress(const char* buf) override {
            CompressCodec codec_type = static_cast<CompressCodec>(*reinterpret_cast<const uint8_t*>(buf));
            buf += sizeof(uint8_t);

            switch (codec_type) {
                case CompressCodec::SAME:
                    decode_from_same(buf);
                    break;
                case CompressCodec::SIMPLE8B:
                    decode_from_simple8B(buf);
                    break;
                case CompressCodec::PLAIN:
                    decode_from_plain(buf);
                    break;
                case CompressCodec::RLE:
                    break;
            }
        }

        void encode_to_same(std::string *buf) const {
            put_fixed(buf, static_cast<uint8_t>(CompressCodec::SAME));
            put_fixed(buf, _column_values[0]);
        }

        void decode_from_same(const char* buf) {
            int32_t same_value = *reinterpret_cast<const int32_t*>(buf);
            std::fill(_column_values.begin(), _column_values.end(), same_value);
        }

        bool encode_to_simple8b(std::string* buf) const {
            const char* uncompress_data = reinterpret_cast<const char*>(_column_values.data());
            uint32_t uncompress_size = DATA_BLOCK_ITEM_NUMS * sizeof(int32_t);
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(uncompress_size * 2);
            uint32_t compress_size = compression::compress_int32(uncompress_data, uncompress_size, compress_data.get());
            if (compress_size >= uncompress_size) {
                return false;
            }
            put_fixed(buf, static_cast<uint8_t>(CompressCodec::SIMPLE8B));
            buf->append((const char*) &uncompress_size, sizeof(uint32_t));
            buf->append((const char*) &compress_size, sizeof(uint32_t));
            buf->append(compress_data.get(), compress_size);
            return true;
        }

        void decode_from_simple8B(const char* buf) {
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + 2 * sizeof(uint32_t), compress_size);
            assert(uncompress_size / sizeof(int32_t) == DATA_BLOCK_ITEM_NUMS);
            char* src = compression::decompress_int32(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);
            std::memcpy(_column_values.data(), src, DATA_BLOCK_ITEM_NUMS * sizeof(int32_t));
        }

        void encode_to_plain(std::string* buf) const {
            put_fixed(buf, static_cast<uint8_t>(CompressCodec::PLAIN));
            buf->append(reinterpret_cast<const char*>(_column_values.data()), DATA_BLOCK_ITEM_NUMS * sizeof(int32_t));
        }

        void decode_from_plain(const char* buf) {
            std::memcpy(_column_values.data(), buf, DATA_BLOCK_ITEM_NUMS * sizeof(int32_t));
        }
    };

    struct DoubleDataBlock : public DataBlock {
        std::array<double_t, DATA_BLOCK_ITEM_NUMS> _column_values;
        bool _is_same = false;

        DoubleDataBlock() = default;

        ~DoubleDataBlock() override = default;

        void encode_to_compress(std::string *buf) const override {
            if (_is_same) {
                encode_to_same(buf);
                return;
            }
            if (encode_to_simple8b(buf)) {
                return;
            }
            encode_to_plain(buf);
        }

        void decode_from_decompress(const char* buf) override {
            CompressCodec codec_type = static_cast<CompressCodec>(*reinterpret_cast<const uint8_t*>(buf));
            buf += sizeof(uint8_t);

            switch (codec_type) {
                case CompressCodec::SAME:
                    decode_from_same(buf);
                    break;
                case CompressCodec::SIMPLE8B:
                    decode_from_simple8B(buf);
                    break;
                case CompressCodec::PLAIN:
                    decode_from_plain(buf);
                    break;
                case CompressCodec::RLE:
                    break;
            }
        }

        void encode_to_same(std::string *buf) const {
            put_fixed(buf, static_cast<uint8_t>(CompressCodec::SAME));
            put_fixed(buf, _column_values[0]);
        }

        void decode_from_same(const char* buf) {
            double_t same_value = *reinterpret_cast<const double_t*>(buf);
            std::fill(_column_values.begin(), _column_values.end(), same_value);
        }

        bool encode_to_simple8b(std::string* buf) const {
            const char* uncompress_data = reinterpret_cast<const char*>(_column_values.data());
            uint32_t uncompress_size = DATA_BLOCK_ITEM_NUMS * sizeof(double_t);
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(uncompress_size * 2);
            uint32_t compress_size = compression::compress_float(uncompress_data, uncompress_size, compress_data.get());
            if (compress_size >= uncompress_size) {
                return false;
            }
            put_fixed(buf, static_cast<uint8_t>(CompressCodec::SIMPLE8B));
            buf->append((const char*) &uncompress_size, sizeof(uint32_t));
            buf->append((const char*) &compress_size, sizeof(uint32_t));
            buf->append(compress_data.get(), compress_size);
            return true;
        }

        void decode_from_simple8B(const char* buf) {
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + 2 * sizeof(uint32_t), compress_size);
            assert(uncompress_size / sizeof(double_t) == DATA_BLOCK_ITEM_NUMS);
            char* src = compression::decompress_float(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);
            std::memcpy(_column_values.data(), src, DATA_BLOCK_ITEM_NUMS * sizeof(double_t));
        }

        void encode_to_plain(std::string* buf) const {
            put_fixed(buf, static_cast<uint8_t>(CompressCodec::PLAIN));
            buf->append(reinterpret_cast<const char*>(_column_values.data()), DATA_BLOCK_ITEM_NUMS * sizeof(double_t));
        }

        void decode_from_plain(const char* buf) {
            std::memcpy(_column_values.data(), buf, DATA_BLOCK_ITEM_NUMS * sizeof(double_t));
        }
    };

    struct StringDataBlock : public DataBlock {
        ColumnValue _column_values[DATA_BLOCK_ITEM_NUMS];

        StringDataBlock() = default;

        ~StringDataBlock() override = default;

        void encode_to_compress(std::string *buf) const override {
            std::string uncompress_buf;

            for (const auto &column_value: _column_values) {
                uncompress_buf.append(column_value.columnData, column_value.getRawDataSize());
            }

            const char* uncompress_data = uncompress_buf.c_str();
            uint32_t uncompress_size = static_cast<uint32_t>(uncompress_buf.size());
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(uncompress_size * 1.2);
            uint32_t compress_size = compression::compress_string(uncompress_data, uncompress_size, compress_data.get());
            buf->append((const char*) &uncompress_size, sizeof(uint32_t));
            buf->append((const char*) &compress_size, sizeof(uint32_t));
            buf->append(compress_data.get(), compress_size);
        }

        void decode_from_decompress(const char* buf) override {
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + 2 * sizeof(uint32_t), compress_size);
            compression::decompress_string(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);
            size_t str_offset = 0;
            uint16_t str_count = 0;

            while (str_offset != uncompress_size) {
                int32_t str_length = *reinterpret_cast<int32_t*>(uncompress_data.get() + str_offset);
                str_offset += sizeof(int32_t);
                _column_values[str_count] = ColumnValue(uncompress_data.get() + str_offset, str_length);
                str_offset += str_length;
                str_count++;
            }

            assert(str_offset == uncompress_size);
            assert(str_count == DATA_BLOCK_ITEM_NUMS);
        }
    };

    // tsm file representation in memory
    struct TsmFile {
        std::vector<std::unique_ptr<DataBlock>> _data_blocks;
        std::vector<IndexBlock> _index_blocks;
        uint32_t _index_offset;

        TsmFile() = default;

        ~TsmFile() = default;

        void encode_to(std::string *buf) {
            size_t index_entry_count = 0;

            for (auto &index_block: _index_blocks) {
                for (auto &index_entry: index_block._index_entries) {
                    index_entry._offset = buf->size();
                    _data_blocks[index_entry_count++]->encode_to_compress(buf);
                    index_entry._size = buf->size() - index_entry._offset;
                }
            }

            assert(index_entry_count == _data_blocks.size());
            _index_offset = buf->size();

            for (const auto &block: _index_blocks) {
                block.encode_to(buf);
            }

            buf->append(reinterpret_cast<const char*>(&_index_offset), sizeof(uint32_t));
        }

        void write_to_file(const Path &tsm_file_path) {
            std::string buf;
            encode_to(&buf);
            io::stream_write_string_to_file(tsm_file_path, buf);
        }

        static void get_size_and_offset(const Path& tsm_file_path, uint32_t* file_size, uint32_t* index_offset) {
            std::ifstream input_file(tsm_file_path, std::ios::binary | std::ios::ate);
            if (!input_file.is_open() || !input_file.good()) {
                throw std::runtime_error("failed to open the file");
            }
            *file_size = input_file.tellg();
            input_file.seekg(-sizeof(uint32_t), std::ios::end);
            input_file.read(reinterpret_cast<char*>(index_offset), sizeof(uint32_t));
            input_file.close();
        }
    };
}