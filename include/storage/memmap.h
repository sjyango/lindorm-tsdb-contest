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

#include <map>
#include <mutex>

#include "Root.h"
#include "struct/Vin.h"
#include "struct/Row.h"
#include "struct/ColumnValue.h"
#include "common/coding.h"

namespace LindormContest::storage {

    const size_t MEMMAP_FLUSH_SIZE = 100000;
    const size_t MEMMAP_SHARD_NUM = 16;

    struct InternalKey {
        Vin _vin;
        std::string _column_name;

        InternalKey(const Vin& vin, const std::string& column_name) : _vin(vin), _column_name(column_name) {}

        ~InternalKey() = default;

        void encode_to(std::string* buf) const {
            buf->append(_vin.vin, VIN_LENGTH);
            put_fixed(buf, (uint8_t) _column_name.size());
            buf->append(_column_name);
        }

        void decode_from(const uint8_t*& buf) {
            std::memcpy(_vin.vin, buf, VIN_LENGTH);
            buf += VIN_LENGTH;
            uint8_t column_name_size = decode_fixed<uint8_t>(buf);
            _column_name.assign(reinterpret_cast<const char*>(buf), column_name_size);
            buf += column_name_size;
        }

        bool operator==(const InternalKey &rhs) const {
            return std::memcmp(_vin.vin, rhs._vin.vin, VIN_LENGTH) == 0 && _column_name == rhs._column_name;
        }

        bool operator!=(const InternalKey &rhs) const {
            return !(rhs == *this);
        }

        bool operator<(const InternalKey &rhs) const {
            int vin_res = std::strncmp(_vin.vin, rhs._vin.vin, VIN_LENGTH);
            if (vin_res != 0) {
                return vin_res < 0;
            }
            return _column_name < rhs._column_name;
        }
    };

    struct InternalValue {
        std::vector<int64_t> _tss;
        std::vector<ColumnValue> _column_values;

        void push_back(int64_t ts, ColumnValue&& column_value) {
            _tss.emplace_back(ts);
            _column_values.emplace_back(std::move(column_value));
        }
    };

    class MemMap {
    public:
        MemMap();

        ~MemMap();

        void append(Row &row);

        void flush();

    private:
        bool _need_flush() const;

        std::map<InternalKey, InternalValue> _mem_map;
        size_t _size;
    };

    class ShardMemMap {
    public:
        ShardMemMap() = default;

        ~ShardMemMap() = default;

        void append(std::vector<Row> &rows);

    private:
        std::mutex _mutexes[MEMMAP_SHARD_NUM];
        MemMap _mem_maps[MEMMAP_SHARD_NUM];
    };

}