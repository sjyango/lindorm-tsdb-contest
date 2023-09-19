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

#include "index_manager.h"
#include "struct/Vin.h"
#include "struct/Row.h"
#include "struct/ColumnValue.h"
#include "struct/Schema.h"
#include "common/coding.h"
#include "common/spinlock.h"
#include "storage/tsm_file.h"

namespace LindormContest {

    struct InternalValue {
        std::vector<int64_t> _tss;
        std::map<std::string, std::vector<ColumnValue>> _values;

        InternalValue() = default;

        InternalValue(const InternalValue& other) = default;

        InternalValue& operator=(const InternalValue& other) = default;

        InternalValue(InternalValue&& other) : _tss(std::move(other._tss)), _values(std::move(other._values)) {}

        ~InternalValue() = default;
    };

    // a mem map for a vin
    // mem map is NOT multi thread safe
    class MemMap {
    public:
        MemMap();

        ~MemMap();

        void append(const Row &row);

        bool empty() const;

        bool need_flush() const;

        void convert(InternalValue& internal_value);

        void flush_to_tsm_file(SchemaSPtr schema, TsmFile& tsm_file);

    private:
        std::vector<Row> _cache;
    };

}