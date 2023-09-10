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
#include <unordered_map>
#include <mutex>

#include "Root.h"
#include "struct/Vin.h"
#include "struct/Row.h"
#include "struct/ColumnValue.h"
#include "struct/Schema.h"
#include "common/coding.h"
#include "common/spinlock.h"
#include "storage/tsm_file.h"

namespace LindormContest {

    const size_t MEMMAP_FLUSH_SIZE = 512;

    struct InternalValue {
        std::vector<std::pair<int64_t, ColumnValue>> _values;

        InternalValue() = default;

        InternalValue(const InternalValue& other) = default;

        InternalValue& operator=(const InternalValue& other) = default;

        InternalValue(InternalValue&& other) : _values(std::move(other._values)) {}

        ~InternalValue() = default;

        void emplace_back(int64_t ts, const ColumnValue& column_value) {
            _values.emplace_back(ts, column_value);
        }

        void sort() {
            std::sort(_values.begin(), _values.end(), [](const auto& lhs, const auto& rhs) {
                return lhs.first < rhs.first;
            });
        }

        size_t size() const {
            return _values.size();
        }
    };

    // a mem map for a vin
    // mem map is NOT multi thread safe
    class MemMap {
    public:
        MemMap();

        ~MemMap();

        void append(const Row &row);

        bool need_flush() const;

        void flush_to_tsm_file(SchemaSPtr schema, TsmFile& tsm_file);

    private:
        size_t _size = 0;
        std::map<std::string, InternalValue> _mem_map;
    };

    // class ShardMemMap {
    // public:
    //     ShardMemMap();
    //
    //     ~ShardMemMap();
    //
    //     void set_root_path(const Path& root_path);
    //
    //     void set_schema(SchemaSPtr schema);
    //
    //     void append(const Row &row);
    //
    // private:
    //     Path _root_path;
    //     SchemaSPtr _schema;
    //     SpinLock _mutex;
    //     std::unordered_map<std::string, std::unique_ptr<MemMap>> _mem_maps;
    // };

}