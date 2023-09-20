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

#include <variant>
#include <optional>

#include "struct/Schema.h"
#include "common/coding.h"
#include "common/thread_pool.h"
#include "common/time_range.h"
#include "compression/compressor.h"
#include "io/io_utils.h"

namespace LindormContest {

    // one index entry corresponds to one data block
    // an index block has a batch of index entry
    struct IndexEntry {
        uint16_t _min_time_index; // inclusive
        uint16_t _max_time_index; // inclusive
        int64_t _min_time; // inclusive
        int64_t _max_time; // inclusive
        char _sum[8]; // int64_t or double_t
        char _max[8]; // int32_t or double_t
        uint16_t _count;
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

        TimeRange get_time_range() const {
            return {_min_time, _max_time + 1};
        }

        void encode_to(std::string *buf, ColumnType type) const {
            put_fixed(buf, _min_time_index);
            put_fixed(buf, _max_time_index);
            put_fixed(buf, _min_time);
            put_fixed(buf, _max_time);
            buf->append(_sum, 8);
            buf->append(_max, 8);
            put_fixed(buf, _count);
            put_fixed(buf, _offset);
            put_fixed(buf, _size);
        }

        void decode_from(const uint8_t *&buf, ColumnType type) {
            _min_time_index = decode_fixed<uint16_t>(buf);
            _max_time_index = decode_fixed<uint16_t>(buf);
            _min_time = decode_fixed<int64_t>(buf);
            _max_time = decode_fixed<int64_t>(buf);
            std::memcpy(_sum, buf, 8);
            buf += 8;
            std::memcpy(_max, buf, 8);
            buf += 8;
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

        bool get_index_entries(const TimeRange& tr, std::vector<IndexEntry>& index_entries) {
            size_t start = std::numeric_limits<size_t>::max(), end = _index_entries.size();
            // find lower index (inclusive)
            for (size_t i = 0; i < _index_entries.size(); ++i) {
                if (tr.overlap(_index_entries[i].get_time_range())) {
                    start = i;
                    break;
                }
            }
            if (start == std::numeric_limits<size_t>::max()) {
                return false;
            }
            // find upper index (exclusive)
            for (size_t i = start + 1; i < _index_entries.size(); ++i) {
                if (!tr.overlap(_index_entries[i].get_time_range())) {
                    end = i;
                    break;
                }
            }
            index_entries.assign(_index_entries.begin() + start, _index_entries.begin() + end);
            return true;
        }

        bool get_max_index_entry(const TimeRange& tr, IndexEntry& index_entry) {
            for (auto it = _index_entries.rbegin(); it != _index_entries.rend(); ++it) {
                if (tr.overlap(it->get_time_range())) {
                    index_entry = *it;
                    return true;
                }
            }
            return false;
        }

        void encode_to(std::string *buf) const {
            _index_meta.encode_to(buf);
            for (const auto &entry: _index_entries) {
                entry.encode_to(buf, _index_meta._type);
            }
        }

        void decode_from(const uint8_t *&buf) {
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

        DataBlock(const std::vector<ColumnValue>::iterator &start,
                  const std::vector<ColumnValue>::iterator &end)
                : _column_values(start, end) {}

        DataBlock(DataBlock &&other) : _column_values(std::move(other._column_values)) {}

        ~DataBlock() = default;

        void encode_to(std::string *buf) const {
            for (const auto &val: _column_values) {
                buf->append(val.columnData, val.getRawDataSize());
            }
        }

        void decode_from(const uint8_t *&buf, ColumnType type, uint16_t count) {
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
                        _column_values.emplace_back((const char *) buf, str_length);
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

        void encode_to(std::string *buf) const {
            buf->append((const char *) _tss.data(), _tss.size() * sizeof(int64_t));
            put_fixed(buf, _index_offset);
            put_fixed(buf, _footer_offset);
        }

        void decode_from(const uint8_t *&buf, size_t ts_count) {
            _tss.resize(ts_count);
            std::memcpy(_tss.data(), buf, ts_count * sizeof(int64_t));
            buf += ts_count * sizeof(int64_t);
            _index_offset = decode_fixed<uint32_t>(buf);
            _footer_offset = decode_fixed<uint32_t>(buf);
        }
    };

    // tsm file representation in memory
    struct TsmFile {
        struct ColumnIterator {
            std::vector<DataBlock>::const_iterator _block_iter;
            std::vector<ColumnValue>::const_iterator _value_iter;

            ColumnIterator(std::vector<DataBlock>::const_iterator block_iter,
                           std::vector<ColumnValue>::const_iterator value_iter)
                    : _block_iter(block_iter), _value_iter(value_iter) {}

            ColumnIterator &operator++() {
                ++_value_iter;
                return *this;
            }

            const ColumnValue &operator*() {
                if (_value_iter == (*_block_iter)._column_values.cend()) {
                    ++_block_iter;
                    _value_iter = (*_block_iter)._column_values.cbegin();
                }
                return *_value_iter;
            }

            bool operator==(const ColumnIterator &other) const {
                return _block_iter == other._block_iter && _value_iter == other._value_iter;
            }

            bool operator!=(const ColumnIterator &other) const {
                return !(*this == other);
            }
        };

        std::vector<DataBlock> _data_blocks;
        std::vector<IndexBlock> _index_blocks;
        Footer _footer;

        TsmFile() = default;

        TsmFile(TsmFile &&other) : _data_blocks(std::move(other._data_blocks)),
                                   _index_blocks(std::move(other._index_blocks)),
                                   _footer(other._footer) {}

        ~TsmFile() = default;

        ColumnIterator column_begin(const std::string &column_name) const;

        ColumnIterator column_end(const std::string &column_name) const;

        void encode_to(std::string *buf);

        void decode_from(const uint8_t *buf, uint32_t file_size);

        void write_to_file(const Path &tsm_file_path);

        void read_from_file(const Path &tsm_file_path);

        static void get_size_and_offset(const Path& tsm_file_path, uint32_t& file_size, uint32_t& index_offset, uint32_t& footer_offset);

        static void get_footer(const Path& tsm_file_path, Footer& footer);
    };
}