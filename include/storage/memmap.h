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

namespace LindormContest {

    const size_t MEMMAP_FLUSH_SIZE = 512;

    struct InternalValue {
        std::vector<int64_t> _tss;
        std::vector<ColumnValue> _column_values;

        void push_back(int64_t ts, const ColumnValue& column_value) {
            _tss.emplace_back(ts);
            _column_values.emplace_back(column_value);
        }

        size_t size() const {
            assert(_tss.size() == _column_values.size());
            return _tss.size();
        }
    };

    // a mem map for a vin
    // mem map is multi thread safe
    class MemMap {
    public:
        MemMap(Path root_path, SchemaSPtr schema, std::string vin_str);

        ~MemMap();

        void append(const Row &row);

        void flush();

        void reset();

        const std::map<std::string, InternalValue>& get_mem_map() const;

    private:
        bool _need_flush() const;

        std::mutex _mutex;
        Path _root_path;
        SchemaSPtr _schema;
        std::string _vin_str;
        size_t _flush_count;
        size_t _size;
        std::map<std::string, InternalValue> _mem_map;
    };

    class ShardMemMap {
    public:
        ShardMemMap();

        ~ShardMemMap();

        void set_root_path(const Path& root_path);

        void set_schema(SchemaSPtr schema);

        void append(const Row &row);

    private:
        Path _root_path;
        SchemaSPtr _schema;
        SpinLock _mutex;
        std::unordered_map<std::string, std::unique_ptr<MemMap>> _mem_maps;
    };

}