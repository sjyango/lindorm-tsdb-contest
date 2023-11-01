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

extern "C" {
#include "../source/bitpacking/include/simdbitpack.h"
}

#include "struct/Schema.h"
#include "common/coding.h"
#include "common/thread_pool.h"
#include "common/time_range.h"
#include "compression/compressor.h"
#include "io/io_utils.h"
#include "../source/pfor/deltautil.h"

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

    // one entry corresponds to one column
    struct IndexBlock {
        std::array<IndexEntry, DATA_BLOCK_COUNT> _index_entries;

        IndexBlock() = default;

        IndexBlock(const IndexBlock& other) = default;

        IndexBlock& operator=(const IndexBlock& other) = default;

        IndexBlock(IndexBlock &&other) noexcept : _index_entries(other._index_entries) {}

        ~IndexBlock() = default;

        void get_index_entries_and_ranges(const TimeRange& file_tr, std::vector<IndexEntry>& index_entries, std::vector<IndexRange>& ranges) {
            for (uint16_t i = file_tr._start_idx / DATA_BLOCK_ITEM_NUMS; i <= file_tr._end_idx / DATA_BLOCK_ITEM_NUMS; ++i) {
                index_entries.emplace_back(_index_entries[i]);
                ranges.emplace_back(std::max(file_tr._start_idx, (uint16_t) (i * DATA_BLOCK_ITEM_NUMS)) % DATA_BLOCK_ITEM_NUMS,
                                    std::min(file_tr._end_idx, (uint16_t) ((i + 1) * DATA_BLOCK_ITEM_NUMS - 1)) % DATA_BLOCK_ITEM_NUMS);
            }
        }

        void encode_to(std::string *buf) const {
            for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                _index_entries[i].encode_to(buf);
            }
        }

        void decode_from(const uint8_t *&buf) {
            for (uint16_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
                _index_entries[i].decode_from(buf);
            }
        }
    };

    enum class IntCompressType : uint8_t {
        SAME,
        BITPACK,
        BITPACK_ZSTD,
        BITPACK_BROTLI,
        SIMPLE8B,
        SIMPLE8B_ZSTD,
        FASTPFOR,
        FASTPFOR_ZSTD,
        FASTPFOR_BROTLI,
        ZSTD,
        PLAIN
    };

    enum class DoubleCompressType : uint8_t {
        SAME,
        GORILLA,
        GORILLA_ZSTD,
        CHIMP,
        CHIMP_ZSTD,
        CHIMP_BROTLI,
        PLAIN
    };

    enum class StringCompressType : uint8_t {
        ZSTD,
        ZSTD_SAME_LENGTH,
        BROTLI,
        BROTLI_SAME_LENGTH,
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
        IntCompressType _type = IntCompressType::FASTPFOR;
        int64_t _sum = 0;
        int32_t _min = std::numeric_limits<int32_t>::max();
        int32_t _max = std::numeric_limits<int32_t>::lowest();
        mutable uint8_t _required_bits; // just for BITPACK

        IntDataBlock() = default;

        ~IntDataBlock() override = default;

        void encode_to_compress(std::string *buf) const override {
            if (_type == IntCompressType::SAME) {
                encode_to_same(buf);
            } else if (_type == IntCompressType::BITPACK) {
                encode_to_bitpack(buf);
            } else if (_type == IntCompressType::SIMPLE8B) {
                if (!encode_to_simple8b(buf)) {
                    if (!encode_to_zstd(buf)) {
                        encode_to_plain(buf);
                    }
                }
            } else if (_type == IntCompressType::FASTPFOR) {
                if (!encode_to_fastpfor(buf)) {
                    if (!encode_to_zstd(buf)) {
                        encode_to_plain(buf);
                    }
                }
            }
        }

        void decode_from_decompress(const char* buf) override {
            IntCompressType type = static_cast<IntCompressType>(*reinterpret_cast<const uint8_t*>(buf));
            buf += sizeof(uint8_t);

            switch (type) {
                case IntCompressType::SAME:
                    decode_from_same(buf);
                    break;
                case IntCompressType::SIMPLE8B:
                    decode_from_simple8b(buf);
                    break;
                case IntCompressType::SIMPLE8B_ZSTD:
                    decode_from_simple8b_zstd(buf);
                    break;
                case IntCompressType::FASTPFOR:
                    decode_from_fastpfor(buf);
                    break;
                case IntCompressType::FASTPFOR_ZSTD:
                    decode_from_fastpfor_zstd(buf);
                    break;
                case IntCompressType::FASTPFOR_BROTLI:
                    decode_from_fastpfor_brotli(buf);
                    break;
                case IntCompressType::ZSTD:
                    decode_from_zstd(buf);
                    break;
                case IntCompressType::BITPACK:
                    decode_from_bitpack(buf);
                    break;
                case IntCompressType::BITPACK_ZSTD:
                    decode_from_bitpack_zstd(buf);
                    break;
                case IntCompressType::BITPACK_BROTLI:
                    decode_from_bitpack_brotli(buf);
                    break;
                case IntCompressType::PLAIN:
                    decode_from_plain(buf);
                    break;
            }
        }

        void encode_to_same(std::string *buf) const {
            put_fixed(buf, static_cast<uint8_t>(IntCompressType::SAME));
            put_fixed(buf, _column_values[0]);
        }

        void decode_from_same(const char* buf) {
            int32_t same_value = *reinterpret_cast<const int32_t*>(buf);
            std::fill(_column_values.begin(), _column_values.end(), same_value);
        }

        bool encode_to_simple8b(std::string* buf) const {
            const char* stage_one_uncompress_data = reinterpret_cast<const char*>(_column_values.data());
            uint32_t stage_one_uncompress_size = DATA_BLOCK_ITEM_NUMS * sizeof(int32_t);
            std::unique_ptr<char[]> stage_one_compress_data = std::make_unique<char[]>(stage_one_uncompress_size * 2);
            uint32_t stage_one_compress_size = compression::compress_int32_simple8b(stage_one_uncompress_data, stage_one_uncompress_size, stage_one_compress_data.get());
            if (stage_one_compress_size >= stage_one_uncompress_size) {
                return false;
            }
            const char* stage_two_uncompress_data = stage_one_compress_data.get();
            uint32_t stage_two_uncompress_size = stage_one_compress_size;
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_uncompress_size * 2);
            uint32_t stage_two_compress_size = compression::compress_string_zstd(stage_two_uncompress_data, stage_two_uncompress_size, stage_two_compress_data.get());
            if (stage_two_compress_size >= stage_two_uncompress_size) {
                put_fixed(buf, static_cast<uint8_t>(IntCompressType::SIMPLE8B));
                buf->append((const char*) &stage_one_uncompress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_one_compress_size, sizeof(uint32_t));
                buf->append(stage_one_compress_data.get(), stage_one_compress_size);
            } else {
                put_fixed(buf, static_cast<uint8_t>(IntCompressType::SIMPLE8B_ZSTD));
                buf->append((const char*) &stage_two_uncompress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_two_compress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_one_uncompress_size, sizeof(uint32_t));
                buf->append(stage_two_compress_data.get(), stage_two_compress_size);
            }
            return true;
        }

        void decode_from_simple8b(const char* buf) {
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + 2 * sizeof(uint32_t), compress_size);
            assert(uncompress_size / sizeof(int32_t) == DATA_BLOCK_ITEM_NUMS);
            char* src = compression::decompress_int32_simple8b(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);
            std::memcpy(_column_values.data(), src, uncompress_size);
        }

        void decode_from_simple8b_zstd(const char* buf) {
            uint32_t stage_two_uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t stage_two_compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            uint32_t stage_one_uncompress_size = *reinterpret_cast<const uint32_t*>(buf + 2 * sizeof(uint32_t));
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_compress_size);
            std::unique_ptr<char[]> stage_two_uncompress_data = std::make_unique<char[]>(stage_two_uncompress_size);
            std::memcpy(stage_two_compress_data.get(), buf + 3 * sizeof(uint32_t), stage_two_compress_size);
            compression::decompress_string_zstd(stage_two_compress_data.get(), stage_two_compress_size, stage_two_uncompress_data.get(), stage_two_uncompress_size);
            std::unique_ptr<char[]> stage_one_uncompress_data = std::make_unique<char[]>(stage_one_uncompress_size);
            assert(stage_one_uncompress_size / sizeof(int32_t) == DATA_BLOCK_ITEM_NUMS);
            const char* stage_one_compress_data = stage_two_uncompress_data.get();
            uint32_t stage_one_compress_size = stage_two_uncompress_size;
            char* src = compression::decompress_int32_simple8b(stage_one_compress_data, stage_one_compress_size, stage_one_uncompress_data.get(), stage_one_uncompress_size);
            std::memcpy(_column_values.data(), src, DATA_BLOCK_ITEM_NUMS * sizeof(int32_t));
        }

        bool encode_to_fastpfor(std::string* buf) const {
            std::array<uint32_t, DATA_BLOCK_ITEM_NUMS> column_values;
            delta_and_zigzag_encode(_column_values.data(), column_values.data(), DATA_BLOCK_ITEM_NUMS);
            const uint32_t * stage_one_uncompress_data = column_values.data();
            uint32_t stage_one_uncompress_size = DATA_BLOCK_ITEM_NUMS;
            std::array<uint32_t, DATA_BLOCK_ITEM_NUMS * 2> stage_one_compress_data;
            uint32_t stage_one_compress_size = compression::compress_int32_fastpfor(stage_one_uncompress_data, stage_one_uncompress_size, stage_one_compress_data.data());

            if (stage_one_compress_size >= stage_one_uncompress_size) {
                return false;
            }

            // INFO_LOG("encode_to_fastpfor, range: %d, uncompress_size: %lu, compress_size: %lu", _max - _min, DATA_BLOCK_ITEM_NUMS * sizeof(uint32_t), stage_one_compress_size * sizeof(uint32_t))
            const char* stage_two_uncompress_data = reinterpret_cast<const char *>(stage_one_compress_data.data());
            uint32_t stage_two_uncompress_size = stage_one_compress_size * sizeof(uint32_t);
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_uncompress_size * 2);
            uint32_t stage_two_compress_size = compression::compress_string_brotli(stage_two_uncompress_data, stage_two_uncompress_size, stage_two_compress_data.get());
            if (stage_two_compress_size >= stage_two_uncompress_size) {
                put_fixed(buf, static_cast<uint8_t>(IntCompressType::FASTPFOR));
                buf->append((const char*) &_min, sizeof(int32_t));
                buf->append((const char*) &stage_one_compress_size, sizeof(uint32_t));
                buf->append(stage_two_uncompress_data, stage_two_uncompress_size);
            } else {
                INFO_LOG("encode_to_fastpfor_brotli, stage_one_uncompress_size: %u, stage_two_uncompress_size: %u, stage_two_compress_size: %u", stage_one_uncompress_size * 4, stage_two_uncompress_size, stage_two_compress_size)
                put_fixed(buf, static_cast<uint8_t>(IntCompressType::FASTPFOR_BROTLI));
                buf->append((const char*) &_min, sizeof(int32_t));
                buf->append((const char*) &stage_two_uncompress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_two_compress_size, sizeof(uint32_t));
                buf->append(stage_two_compress_data.get(), stage_two_compress_size);
            }
            return true;
        }

        void decode_from_fastpfor(const char* buf) {
            _min = *reinterpret_cast<const int32_t*>(buf);
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(int32_t));
            std::vector<uint32_t> compress_data(compress_size);
            std::array<uint32_t, DATA_BLOCK_ITEM_NUMS> uncompress_data;
            std::memcpy(compress_data.data(), buf + 2 * sizeof(uint32_t), compress_size * sizeof(uint32_t));
            uint32_t decompress_size = compression::decompress_int32_fastpfor(compress_data.data(), compress_size, uncompress_data.data());
            assert(decompress_size == DATA_BLOCK_ITEM_NUMS);
            delta_and_zigzag_decode(uncompress_data.data(), _column_values.data(), DATA_BLOCK_ITEM_NUMS);
        }

        void decode_from_fastpfor_zstd(const char* buf) {
            _min = *reinterpret_cast<const int32_t*>(buf);
            uint32_t stage_two_uncompress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(int32_t));
            uint32_t stage_two_compress_size = *reinterpret_cast<const uint32_t*>(buf + 2 * sizeof(uint32_t));
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_compress_size);
            std::unique_ptr<char[]> stage_two_uncompress_data = std::make_unique<char[]>(stage_two_uncompress_size);
            std::memcpy(stage_two_compress_data.get(), buf + 3 * sizeof(uint32_t), stage_two_compress_size);
            compression::decompress_string_zstd(stage_two_compress_data.get(), stage_two_compress_size, stage_two_uncompress_data.get(), stage_two_uncompress_size);
            std::array<uint32_t, DATA_BLOCK_ITEM_NUMS> stage_one_uncompress_data;
            const uint32_t * stage_one_compress_data = reinterpret_cast<const uint32_t *>(stage_two_uncompress_data.get());
            uint32_t stage_one_compress_size = stage_two_uncompress_size / sizeof(uint32_t);
            uint32_t decompress_size = compression::decompress_int32_fastpfor(stage_one_compress_data, stage_one_compress_size, stage_one_uncompress_data.data());
            assert(decompress_size == DATA_BLOCK_ITEM_NUMS);
            delta_and_zigzag_decode(stage_one_uncompress_data.data(), _column_values.data(), DATA_BLOCK_ITEM_NUMS);
        }

        void decode_from_fastpfor_brotli(const char* buf) {
            _min = *reinterpret_cast<const int32_t*>(buf);
            uint32_t stage_two_uncompress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(int32_t));
            uint32_t stage_two_compress_size = *reinterpret_cast<const uint32_t*>(buf + 2 * sizeof(uint32_t));
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_compress_size);
            std::unique_ptr<char[]> stage_two_uncompress_data = std::make_unique<char[]>(stage_two_uncompress_size);
            std::memcpy(stage_two_compress_data.get(), buf + 3 * sizeof(uint32_t), stage_two_compress_size);
            compression::decompress_string_brotli(stage_two_compress_data.get(), stage_two_compress_size, stage_two_uncompress_data.get(), stage_two_uncompress_size);
            std::array<uint32_t, DATA_BLOCK_ITEM_NUMS> stage_one_uncompress_data;
            const uint32_t * stage_one_compress_data = reinterpret_cast<const uint32_t *>(stage_two_uncompress_data.get());
            uint32_t stage_one_compress_size = stage_two_uncompress_size / sizeof(uint32_t);
            uint32_t decompress_size = compression::decompress_int32_fastpfor(stage_one_compress_data, stage_one_compress_size, stage_one_uncompress_data.data());
            assert(decompress_size == DATA_BLOCK_ITEM_NUMS);
            delta_and_zigzag_decode(stage_one_uncompress_data.data(), _column_values.data(), DATA_BLOCK_ITEM_NUMS);
        }

        void encode_to_bitpack(std::string* buf) const {
            _required_bits = get_next_power_of_two(_max - _min + 1);
            std::array<uint32_t, DATA_BLOCK_ITEM_NUMS> stage_one_uncompress_data;

            for (uint16_t i = 0; i < DATA_BLOCK_ITEM_NUMS; ++i) {
                stage_one_uncompress_data[i] = _column_values[i] - _min;
            }

            uint32_t stage_one_uncompress_size = DATA_BLOCK_ITEM_NUMS * sizeof(uint32_t);
            std::unique_ptr<char[]> stage_one_compress_data = std::make_unique<char[]>(stage_one_uncompress_size);
            __m128i* end_buf = simdpack_length(stage_one_uncompress_data.data(), DATA_BLOCK_ITEM_NUMS, (__m128i*) stage_one_compress_data.get(), _required_bits);
            uint32_t stage_one_compress_size = (end_buf - (__m128i *) stage_one_compress_data.get()) * sizeof(__m128i);
            const char* stage_two_uncompress_data = stage_one_compress_data.get();
            uint32_t stage_two_uncompress_size = stage_one_compress_size;
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_uncompress_size * 2);
            uint32_t stage_two_compress_size = compression::compress_string_brotli(stage_two_uncompress_data, stage_two_uncompress_size, stage_two_compress_data.get());

            if (stage_two_compress_size >= stage_two_uncompress_size) {
                put_fixed(buf, static_cast<uint8_t>(IntCompressType::BITPACK));
                put_fixed(buf, _required_bits);
                put_fixed(buf, _min);
                buf->append((const char*) &stage_one_compress_size, sizeof(uint32_t));
                buf->append(stage_one_compress_data.get(), stage_one_compress_size);
            } else {
                // INFO_LOG("encode_to_bitpack_brotli, range: %d, stage_one_uncompress_size: %u, stage_two_uncompress_size: %u, stage_two_compress_size: %u", _max - _min + 1, stage_one_uncompress_size, stage_two_uncompress_size, stage_two_compress_size)
                put_fixed(buf, static_cast<uint8_t>(IntCompressType::BITPACK_BROTLI));
                put_fixed(buf, _required_bits);
                put_fixed(buf, _min);
                buf->append((const char*) &stage_two_uncompress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_two_compress_size, sizeof(uint32_t));
                buf->append(stage_two_compress_data.get(), stage_two_compress_size);
            }
        }

        void decode_from_bitpack(const char* buf) {
            _required_bits = *reinterpret_cast<const uint8_t*>(buf);
            _min = *reinterpret_cast<const int32_t*>(buf + sizeof(uint8_t));
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint8_t) + sizeof(int32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::memcpy(compress_data.get(), buf + sizeof(uint8_t) + sizeof(int32_t) + sizeof(uint32_t), compress_size);
            std::array<uint32_t, DATA_BLOCK_ITEM_NUMS> uncompress_data;
            simdunpack_length((const __m128i*) compress_data.get(), DATA_BLOCK_ITEM_NUMS, uncompress_data.data(), _required_bits);

            for (uint16_t i = 0; i < DATA_BLOCK_ITEM_NUMS; ++i) {
                _column_values[i] = uncompress_data[i] + _min;
            }
        }

        void decode_from_bitpack_zstd(const char* buf) {
            _required_bits = *reinterpret_cast<const uint8_t*>(buf);
            _min = *reinterpret_cast<const int32_t*>(buf + sizeof(uint8_t));
            uint32_t stage_two_uncompress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint8_t) + sizeof(int32_t));
            uint32_t stage_two_compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint8_t) + 2 * sizeof(uint32_t));
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_compress_size);
            std::unique_ptr<char[]> stage_two_uncompress_data = std::make_unique<char[]>(stage_two_uncompress_size);
            std::memcpy(stage_two_compress_data.get(), buf + sizeof(uint8_t) + 3 * sizeof(uint32_t), stage_two_compress_size);
            compression::decompress_string_zstd(stage_two_compress_data.get(), stage_two_compress_size, stage_two_uncompress_data.get(), stage_two_uncompress_size);
            const char* stage_one_compress_data = stage_two_uncompress_data.get();
            std::array<uint32_t, DATA_BLOCK_ITEM_NUMS> stage_one_uncompress_data;
            simdunpack_length((const __m128i*) stage_one_compress_data, DATA_BLOCK_ITEM_NUMS, stage_one_uncompress_data.data(), _required_bits);

            for (uint16_t i = 0; i < DATA_BLOCK_ITEM_NUMS; ++i) {
                _column_values[i] = stage_one_uncompress_data[i] + _min;
            }
        }

        void decode_from_bitpack_brotli(const char* buf) {
            _required_bits = *reinterpret_cast<const uint8_t*>(buf);
            _min = *reinterpret_cast<const int32_t*>(buf + sizeof(uint8_t));
            uint32_t stage_two_uncompress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint8_t) + sizeof(int32_t));
            uint32_t stage_two_compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint8_t) + 2 * sizeof(uint32_t));
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_compress_size);
            std::unique_ptr<char[]> stage_two_uncompress_data = std::make_unique<char[]>(stage_two_uncompress_size);
            std::memcpy(stage_two_compress_data.get(), buf + sizeof(uint8_t) + 3 * sizeof(uint32_t), stage_two_compress_size);
            compression::decompress_string_brotli(stage_two_compress_data.get(), stage_two_compress_size, stage_two_uncompress_data.get(), stage_two_uncompress_size);
            const char* stage_one_compress_data = stage_two_uncompress_data.get();
            std::array<uint32_t, DATA_BLOCK_ITEM_NUMS> stage_one_uncompress_data;
            simdunpack_length((const __m128i*) stage_one_compress_data, DATA_BLOCK_ITEM_NUMS, stage_one_uncompress_data.data(), _required_bits);

            for (uint16_t i = 0; i < DATA_BLOCK_ITEM_NUMS; ++i) {
                _column_values[i] = stage_one_uncompress_data[i] + _min;
            }
        }

        bool encode_to_zstd(std::string* buf) const {
            const char* uncompress_data = reinterpret_cast<const char *>(_column_values.data());
            uint32_t uncompress_size = static_cast<uint32_t>(DATA_BLOCK_ITEM_NUMS * sizeof(int32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(uncompress_size * 1.2);
            uint32_t compress_size = compression::compress_string_zstd(uncompress_data, uncompress_size, compress_data.get());
            if (compress_size >= uncompress_size) {
                return false;
            }
            put_fixed(buf, static_cast<uint8_t>(IntCompressType::ZSTD));
            buf->append((const char*) &uncompress_size, sizeof(uint32_t));
            buf->append((const char*) &compress_size, sizeof(uint32_t));
            buf->append(compress_data.get(), compress_size);
            return true;
        }

        void decode_from_zstd(const char* buf) {
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + 2 * sizeof(uint32_t), compress_size);
            compression::decompress_string_zstd(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);
            assert(uncompress_size == DATA_BLOCK_ITEM_NUMS * sizeof(int32_t));
            std::memcpy(_column_values.data(), uncompress_data.get(), uncompress_size);
        }

        void encode_to_plain(std::string* buf) const {
            put_fixed(buf, static_cast<uint8_t>(IntCompressType::PLAIN));
            buf->append(reinterpret_cast<const char*>(_column_values.data()), DATA_BLOCK_ITEM_NUMS * sizeof(int32_t));
        }

        void decode_from_plain(const char* buf) {
            std::memcpy(_column_values.data(), buf, DATA_BLOCK_ITEM_NUMS * sizeof(int32_t));
        }
    };

    struct DoubleDataBlock : public DataBlock {
        std::array<double_t, DATA_BLOCK_ITEM_NUMS> _column_values;
        DoubleCompressType _type = DoubleCompressType::CHIMP;
        double_t _sum = 0.0;
        double_t _min = std::numeric_limits<double_t>::max();
        double_t _max = std::numeric_limits<double_t>::lowest();

        DoubleDataBlock() = default;

        ~DoubleDataBlock() override = default;

        void encode_to_compress(std::string *buf) const override {
            if (_type == DoubleCompressType::SAME) {
                encode_to_same(buf);
            } else if (_type == DoubleCompressType::GORILLA) {
                if (!encode_to_gorilla(buf)) {
                    encode_to_plain(buf);
                }
            } else if (_type == DoubleCompressType::CHIMP) {
                if (!encode_to_chimp(buf)) {
                    encode_to_plain(buf);
                }
            }
        }

        void decode_from_decompress(const char* buf) override {
            DoubleCompressType type = static_cast<DoubleCompressType>(*reinterpret_cast<const uint8_t*>(buf));
            buf += sizeof(uint8_t);

            switch (type) {
                case DoubleCompressType::SAME:
                    decode_from_same(buf);
                    break;
                case DoubleCompressType::GORILLA:
                    decode_from_gorilla(buf);
                    break;
                case DoubleCompressType::GORILLA_ZSTD:
                    decode_from_gorilla_zstd(buf);
                    break;
                case DoubleCompressType::CHIMP:
                    decode_from_chimp(buf);
                    break;
                case DoubleCompressType::CHIMP_ZSTD:
                    decode_from_chimp_zstd(buf);
                    break;
                case DoubleCompressType::CHIMP_BROTLI:
                    decode_from_chimp_brotli(buf);
                    break;
                case DoubleCompressType::PLAIN:
                    decode_from_plain(buf);
                    break;
            }
        }

        void encode_to_same(std::string *buf) const {
            put_fixed(buf, static_cast<uint8_t>(DoubleCompressType::SAME));
            put_fixed(buf, _column_values[0]);
        }

        void decode_from_same(const char* buf) {
            double_t same_value = *reinterpret_cast<const double_t*>(buf);
            std::fill(_column_values.begin(), _column_values.end(), same_value);
        }

        bool encode_to_gorilla(std::string* buf) const {
            const char* stage_one_uncompress_data = reinterpret_cast<const char*>(_column_values.data());
            uint32_t stage_one_uncompress_size = DATA_BLOCK_ITEM_NUMS * sizeof(double_t);
            std::unique_ptr<char[]> stage_one_compress_data = std::make_unique<char[]>(stage_one_uncompress_size * 2);
            uint32_t stage_one_compress_size = compression::compress_double_gorilla(stage_one_uncompress_data, stage_one_uncompress_size, stage_one_compress_data.get());

            if (stage_one_compress_size >= stage_one_uncompress_size) {
                return false;
            }

            const char* stage_two_uncompress_data = stage_one_compress_data.get();
            uint32_t stage_two_uncompress_size = stage_one_compress_size;
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_uncompress_size * 2);
            uint32_t stage_two_compress_size = compression::compress_string_zstd(stage_two_uncompress_data, stage_two_uncompress_size, stage_two_compress_data.get());
            if (stage_two_compress_size >= stage_two_uncompress_size) {
                put_fixed(buf, static_cast<uint8_t>(DoubleCompressType::GORILLA));
                buf->append((const char*) &stage_one_uncompress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_one_compress_size, sizeof(uint32_t));
                buf->append(stage_one_compress_data.get(), stage_one_compress_size);
            } else {
                // INFO_LOG("encode_to_gorilla_zstd, stage_one_uncompress_size: %u, stage_two_uncompress_size: %u, stage_two_compress_size: %u", stage_one_uncompress_size, stage_two_uncompress_size, stage_two_compress_size)
                put_fixed(buf, static_cast<uint8_t>(DoubleCompressType::GORILLA_ZSTD));
                buf->append((const char*) &stage_two_uncompress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_two_compress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_one_uncompress_size, sizeof(uint32_t));
                buf->append(stage_two_compress_data.get(), stage_two_compress_size);
            }
            return true;
        }

        void decode_from_gorilla(const char* buf) {
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + 2 * sizeof(uint32_t), compress_size);
            assert(uncompress_size / sizeof(double_t) == DATA_BLOCK_ITEM_NUMS);
            char* src = compression::decompress_double_gorilla(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);
            std::memcpy(_column_values.data(), src, uncompress_size);
        }

        void decode_from_gorilla_zstd(const char* buf) {
            uint32_t stage_two_uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t stage_two_compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            uint32_t stage_one_uncompress_size = *reinterpret_cast<const uint32_t*>(buf + 2 * sizeof(uint32_t));
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_compress_size);
            std::unique_ptr<char[]> stage_two_uncompress_data = std::make_unique<char[]>(stage_two_uncompress_size);
            std::memcpy(stage_two_compress_data.get(), buf + 3 * sizeof(uint32_t), stage_two_compress_size);
            compression::decompress_string_zstd(stage_two_compress_data.get(), stage_two_compress_size, stage_two_uncompress_data.get(), stage_two_uncompress_size);
            std::unique_ptr<char[]> stage_one_uncompress_data = std::make_unique<char[]>(stage_one_uncompress_size);
            assert(stage_one_uncompress_size / sizeof(double_t) == DATA_BLOCK_ITEM_NUMS);
            const char* stage_one_compress_data = stage_two_uncompress_data.get();
            uint32_t stage_one_compress_size = stage_two_uncompress_size;
            char* src = compression::decompress_double_gorilla(stage_one_compress_data, stage_one_compress_size, stage_one_uncompress_data.get(), stage_one_uncompress_size);
            std::memcpy(_column_values.data(), src, DATA_BLOCK_ITEM_NUMS * sizeof(double_t));
        }

        bool encode_to_chimp(std::string* buf) const {
            const char* stage_one_uncompress_data = reinterpret_cast<const char*>(_column_values.data());
            uint32_t stage_one_uncompress_size = DATA_BLOCK_ITEM_NUMS * sizeof(double_t);
            std::unique_ptr<char[]> stage_one_compress_data = std::make_unique<char[]>(stage_one_uncompress_size * 2);
            uint32_t stage_one_compress_size = compression::compress_double_chimp(stage_one_uncompress_data, stage_one_uncompress_size, stage_one_compress_data.get());

            if (stage_one_compress_size >= stage_one_uncompress_size) {
                return false;
            }

            const char* stage_two_uncompress_data = stage_one_compress_data.get();
            uint32_t stage_two_uncompress_size = stage_one_compress_size;
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_uncompress_size * 2);
            uint32_t stage_two_compress_size = compression::compress_string_brotli(stage_two_uncompress_data, stage_two_uncompress_size, stage_two_compress_data.get());
            if (stage_two_compress_size >= stage_two_uncompress_size) {
                put_fixed(buf, static_cast<uint8_t>(DoubleCompressType::CHIMP));
                buf->append((const char*) &stage_one_uncompress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_one_compress_size, sizeof(uint32_t));
                buf->append(stage_one_compress_data.get(), stage_one_compress_size);
            } else {
                // INFO_LOG("encode_to_chimp_brotli, stage_one_uncompress_size: %u, stage_two_uncompress_size: %u, stage_two_compress_size: %u", stage_one_uncompress_size, stage_two_uncompress_size, stage_two_compress_size)
                put_fixed(buf, static_cast<uint8_t>(DoubleCompressType::CHIMP_BROTLI));
                buf->append((const char*) &stage_two_uncompress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_two_compress_size, sizeof(uint32_t));
                buf->append((const char*) &stage_one_uncompress_size, sizeof(uint32_t));
                buf->append(stage_two_compress_data.get(), stage_two_compress_size);
            }
            return true;
        }

        void decode_from_chimp(const char* buf) {
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + 2 * sizeof(uint32_t), compress_size);
            assert(uncompress_size / sizeof(double_t) == DATA_BLOCK_ITEM_NUMS);
            char* src = compression::decompress_double_chimp(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);
            std::memcpy(_column_values.data(), src, uncompress_size);
        }

        void decode_from_chimp_zstd(const char* buf) {
            uint32_t stage_two_uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t stage_two_compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            uint32_t stage_one_uncompress_size = *reinterpret_cast<const uint32_t*>(buf + 2 * sizeof(uint32_t));
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_compress_size);
            std::unique_ptr<char[]> stage_two_uncompress_data = std::make_unique<char[]>(stage_two_uncompress_size);
            std::memcpy(stage_two_compress_data.get(), buf + 3 * sizeof(uint32_t), stage_two_compress_size);
            compression::decompress_string_zstd(stage_two_compress_data.get(), stage_two_compress_size, stage_two_uncompress_data.get(), stage_two_uncompress_size);
            std::unique_ptr<char[]> stage_one_uncompress_data = std::make_unique<char[]>(stage_one_uncompress_size);
            assert(stage_one_uncompress_size / sizeof(double_t) == DATA_BLOCK_ITEM_NUMS);
            const char* stage_one_compress_data = stage_two_uncompress_data.get();
            uint32_t stage_one_compress_size = stage_two_uncompress_size;
            char* src = compression::decompress_double_chimp(stage_one_compress_data, stage_one_compress_size, stage_one_uncompress_data.get(), stage_one_uncompress_size);
            std::memcpy(_column_values.data(), src, DATA_BLOCK_ITEM_NUMS * sizeof(double_t));
        }

        void decode_from_chimp_brotli(const char* buf) {
            uint32_t stage_two_uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t stage_two_compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            uint32_t stage_one_uncompress_size = *reinterpret_cast<const uint32_t*>(buf + 2 * sizeof(uint32_t));
            std::unique_ptr<char[]> stage_two_compress_data = std::make_unique<char[]>(stage_two_compress_size);
            std::unique_ptr<char[]> stage_two_uncompress_data = std::make_unique<char[]>(stage_two_uncompress_size);
            std::memcpy(stage_two_compress_data.get(), buf + 3 * sizeof(uint32_t), stage_two_compress_size);
            compression::decompress_string_brotli(stage_two_compress_data.get(), stage_two_compress_size, stage_two_uncompress_data.get(), stage_two_uncompress_size);
            std::unique_ptr<char[]> stage_one_uncompress_data = std::make_unique<char[]>(stage_one_uncompress_size);
            assert(stage_one_uncompress_size / sizeof(double_t) == DATA_BLOCK_ITEM_NUMS);
            const char* stage_one_compress_data = stage_two_uncompress_data.get();
            uint32_t stage_one_compress_size = stage_two_uncompress_size;
            char* src = compression::decompress_double_chimp(stage_one_compress_data, stage_one_compress_size, stage_one_uncompress_data.get(), stage_one_uncompress_size);
            std::memcpy(_column_values.data(), src, DATA_BLOCK_ITEM_NUMS * sizeof(double_t));
        }

        void encode_to_plain(std::string* buf) const {
            put_fixed(buf, static_cast<uint8_t>(DoubleCompressType::PLAIN));
            buf->append(reinterpret_cast<const char*>(_column_values.data()), DATA_BLOCK_ITEM_NUMS * sizeof(double_t));
        }

        void decode_from_plain(const char* buf) {
            std::memcpy(_column_values.data(), buf, DATA_BLOCK_ITEM_NUMS * sizeof(double_t));
        }
    };

    struct StringDataBlock : public DataBlock {
        std::array<ColumnValue, DATA_BLOCK_ITEM_NUMS> _column_values;
        StringCompressType _type = StringCompressType::ZSTD;
        int32_t _min_length = std::numeric_limits<int32_t>::max();
        int32_t _max_length = std::numeric_limits<int32_t>::lowest();

        StringDataBlock() = default;

        ~StringDataBlock() override = default;

        void encode_to_compress(std::string *buf) const override {
            std::string uncompress_buf;
            if (_type == StringCompressType::ZSTD) {
                if (!encode_to_zstd(buf, uncompress_buf)) {
                    encode_to_plain(uncompress_buf, buf);
                }
            } else if (_type == StringCompressType::ZSTD_SAME_LENGTH) {
                if (!encode_to_zstd_same_length(buf, uncompress_buf)) {
                    encode_to_plain(uncompress_buf, buf);
                }
            } else if (_type == StringCompressType::BROTLI) {
                if (!encode_to_brotli(buf, uncompress_buf)) {
                    encode_to_plain(uncompress_buf, buf);
                }
            } else if (_type == StringCompressType::BROTLI_SAME_LENGTH) {
                if (!encode_to_brotli_same_length(buf, uncompress_buf)) {
                    encode_to_plain(uncompress_buf, buf);
                }
            }
        }

        void decode_from_decompress(const char* buf) override {
            StringCompressType type = static_cast<StringCompressType>(*reinterpret_cast<const uint8_t*>(buf));
            buf += sizeof(uint8_t);

            switch (type) {
                case StringCompressType::ZSTD:
                    decode_from_zstd(buf);
                    break;
                case StringCompressType::ZSTD_SAME_LENGTH:
                    decode_from_zstd_same_length(buf);
                    break;
                case StringCompressType::BROTLI:
                    decode_from_brotli(buf);
                    break;
                case StringCompressType::BROTLI_SAME_LENGTH:
                    decode_from_brotli_same_length(buf);
                    break;
                case StringCompressType::PLAIN:
                    decode_from_plain(buf);
                    break;
            }
        }

        bool encode_to_zstd(std::string *buf, std::string& uncompress_buf) const {
            uncompress_buf.resize(DATA_BLOCK_ITEM_NUMS);

            for (uint16_t i = 0; i < DATA_BLOCK_ITEM_NUMS; ++i) {
                uint8_t str_length = static_cast<uint8_t>(*reinterpret_cast<int32_t*>(_column_values[i].columnData));
                *reinterpret_cast<uint8_t*>(uncompress_buf.data() + i) = str_length;
                uncompress_buf.append(_column_values[i].columnData + sizeof(int32_t), str_length);
            }

            const char* uncompress_data = uncompress_buf.c_str();
            uint32_t uncompress_size = static_cast<uint32_t>(uncompress_buf.size());
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(uncompress_size * 1.2);
            uint32_t compress_size = compression::compress_string_zstd(uncompress_data, uncompress_size, compress_data.get());

            if (compress_size >= uncompress_size) {
                return false;
            }

            // INFO_LOG("string encode_to_zstd, uncompress_size: %u, compress_size: %u", uncompress_size, compress_size)

            put_fixed(buf, static_cast<uint8_t>(StringCompressType::ZSTD));
            buf->append((const char*) &uncompress_size, sizeof(uint32_t));
            buf->append((const char*) &compress_size, sizeof(uint32_t));
            buf->append(compress_data.get(), compress_size);
            return true;
        }

        void decode_from_zstd(const char* buf) {
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + 2 * sizeof(uint32_t), compress_size);
            compression::decompress_string_zstd(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);

            std::array<uint8_t, DATA_BLOCK_ITEM_NUMS> str_lengths;
            std::memcpy(str_lengths.data(), uncompress_data.get(), DATA_BLOCK_ITEM_NUMS);
            const char* str_offset = uncompress_data.get() + DATA_BLOCK_ITEM_NUMS;

            for (uint16_t i = 0; i < DATA_BLOCK_ITEM_NUMS; ++i) {
                _column_values[i] = ColumnValue(str_offset, str_lengths[i]);
                str_offset += str_lengths[i];
            }

            assert(str_offset == uncompress_data.get() + uncompress_size);
        }

        bool encode_to_zstd_same_length(std::string *buf, std::string& uncompress_buf) const {
            if (unlikely(_min_length == 0)) {
                put_fixed(buf, static_cast<uint8_t>(StringCompressType::ZSTD_SAME_LENGTH));
                uint8_t str_length = static_cast<uint8_t>(_min_length);
                buf->append((const char*) &str_length, sizeof(uint8_t));
                return true;
            }

            for (const auto &column_value: _column_values) {
                uncompress_buf.append(column_value.columnData + sizeof(int32_t), _min_length);
            }

            const char* uncompress_data = uncompress_buf.c_str();
            uint32_t uncompress_size = static_cast<uint32_t>(uncompress_buf.size());
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(uncompress_size * 1.2);
            uint32_t compress_size = compression::compress_string_zstd(uncompress_data, uncompress_size, compress_data.get());

            if (compress_size >= uncompress_size) {
                return false;
            }

            // INFO_LOG("string encode_to_zstd_same_length, uncompress_size: %u, compress_size: %u", uncompress_size, compress_size)

            put_fixed(buf, static_cast<uint8_t>(StringCompressType::ZSTD_SAME_LENGTH));
            uint8_t str_length = static_cast<uint8_t>(_min_length);
            buf->append((const char*) &str_length, sizeof(uint8_t));
            buf->append((const char*) &uncompress_size, sizeof(uint32_t));
            buf->append((const char*) &compress_size, sizeof(uint32_t));
            buf->append(compress_data.get(), compress_size);
            return true;
        }

        void decode_from_zstd_same_length(const char* buf) {
            uint8_t str_length = *reinterpret_cast<const uint8_t*>(buf);

            if (unlikely(str_length == 0)) {
                for (uint16_t i = 0; i < DATA_BLOCK_ITEM_NUMS; ++i) {
                    _column_values[i] = ColumnValue(buf, 0);
                }
                return;
            }

            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint8_t));
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint8_t) + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + sizeof(uint8_t) + 2 * sizeof(uint32_t), compress_size);
            compression::decompress_string_zstd(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);

            for (uint16_t i = 0; i < DATA_BLOCK_ITEM_NUMS; ++i) {
                _column_values[i] = ColumnValue(uncompress_data.get() + i * str_length, str_length);
            }
        }

        bool encode_to_brotli(std::string *buf, std::string& uncompress_buf) const {
            for (const auto &column_value: _column_values) {
                uint8_t str_length = static_cast<uint8_t>(*reinterpret_cast<int32_t*>(column_value.columnData));
                uncompress_buf.append((const char*) &str_length, sizeof(uint8_t));
                uncompress_buf.append(column_value.columnData + sizeof(int32_t), str_length);
            }

            const char* uncompress_data = uncompress_buf.c_str();
            uint32_t uncompress_size = static_cast<uint32_t>(uncompress_buf.size());
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(uncompress_size * 1.2);
            uint32_t compress_size = compression::compress_string_brotli(uncompress_data, uncompress_size, compress_data.get());

            if (compress_size >= uncompress_size) {
                return false;
            }

            // INFO_LOG("string encode_to_brotli, uncompress_size: %u, compress_size: %u", uncompress_size, compress_size)

            put_fixed(buf, static_cast<uint8_t>(StringCompressType::BROTLI));
            buf->append((const char*) &uncompress_size, sizeof(uint32_t));
            buf->append((const char*) &compress_size, sizeof(uint32_t));
            buf->append(compress_data.get(), compress_size);
            return true;
        }

        void decode_from_brotli(const char* buf) {
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + 2 * sizeof(uint32_t), compress_size);
            compression::decompress_string_brotli(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);
            size_t str_offset = 0;
            uint16_t str_count = 0;

            while (str_offset != uncompress_size) {
                uint8_t str_length = *reinterpret_cast<uint8_t*>(uncompress_data.get() + str_offset);
                str_offset += sizeof(uint8_t);
                _column_values[str_count] = ColumnValue(uncompress_data.get() + str_offset, str_length);
                str_offset += str_length;
                str_count++;
            }

            assert(str_offset == uncompress_size);
            assert(str_count == DATA_BLOCK_ITEM_NUMS);
        }

        bool encode_to_brotli_same_length(std::string *buf, std::string& uncompress_buf) const {
            for (const auto &column_value: _column_values) {
                uncompress_buf.append(column_value.columnData + sizeof(int32_t), _min_length);
            }

            const char* uncompress_data = uncompress_buf.c_str();
            uint32_t uncompress_size = static_cast<uint32_t>(uncompress_buf.size());
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(uncompress_size * 1.2);
            uint32_t compress_size = compression::compress_string_brotli(uncompress_data, uncompress_size, compress_data.get());

            if (compress_size >= uncompress_size) {
                return false;
            }

            // INFO_LOG("string encode_to_brotli, uncompress_size: %u, compress_size: %u", uncompress_size, compress_size)

            put_fixed(buf, static_cast<uint8_t>(StringCompressType::BROTLI_SAME_LENGTH));
            uint8_t str_length = static_cast<uint8_t>(_min_length);
            buf->append((const char*) &str_length, sizeof(uint8_t));
            buf->append((const char*) &uncompress_size, sizeof(uint32_t));
            buf->append((const char*) &compress_size, sizeof(uint32_t));
            buf->append(compress_data.get(), compress_size);
            return true;
        }

        void decode_from_brotli_same_length(const char* buf) {
            uint32_t str_length = *reinterpret_cast<const uint8_t*>(buf);
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint8_t));
            uint32_t compress_size = *reinterpret_cast<const uint32_t*>(buf + sizeof(uint8_t) + sizeof(uint32_t));
            std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(compress_size);
            std::unique_ptr<char[]> uncompress_data = std::make_unique<char[]>(uncompress_size);
            std::memcpy(compress_data.get(), buf + sizeof(uint8_t) + 2 * sizeof(uint32_t), compress_size);
            compression::decompress_string_brotli(compress_data.get(), compress_size, uncompress_data.get(), uncompress_size);

            for (uint16_t i = 0; i < DATA_BLOCK_ITEM_NUMS; ++i) {
                _column_values[i] = ColumnValue(uncompress_data.get() + i * str_length, str_length);
            }
        }

        void encode_to_plain(const std::string& uncompress_buf, std::string* buf) const {
            put_fixed(buf, static_cast<uint8_t>(StringCompressType::PLAIN));
            uint32_t uncompress_size = static_cast<uint32_t>(uncompress_buf.size());
            buf->append((const char*) &uncompress_size, sizeof(uint32_t));
            buf->append(uncompress_buf.c_str(), uncompress_size);
        }

        void decode_from_plain(const char* buf) {
            uint32_t uncompress_size = *reinterpret_cast<const uint32_t*>(buf);
            std::string uncompress_buf;
            uncompress_buf.resize(uncompress_size);
            std::memcpy(uncompress_buf.data(), buf + sizeof(uint32_t), uncompress_size);
            size_t str_offset = 0;
            uint16_t str_count = 0;

            while (str_offset != uncompress_size) {
                uint8_t str_length = *reinterpret_cast<uint8_t*>(uncompress_buf.data() + str_offset);
                str_offset += sizeof(uint8_t);
                _column_values[str_count] = ColumnValue(uncompress_buf.data() + str_offset, str_length);
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
            buf.reserve(10 * 1024 * 1024);
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