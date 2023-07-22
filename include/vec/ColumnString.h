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

#include "Root.h"
#include "vec/IColumn.h"

namespace LindormContest::vectorized {

class ColumnString : public IColumn {
public:
    using Char = UInt8;
    using Chars = std::vector<Char>;

    ColumnString() = default;

    ColumnString(const ColumnString& src)
            : IColumn(src.get_name()), offsets(src.offsets.begin(), src.offsets.end()),
              chars(src.chars.begin(), src.chars.end()) {}

    ColumnType get_type() const {
        return ColumnType::COLUMN_TYPE_STRING;
    }

    inline size_t offset_at(size_t i) const {
        return offsets[i - 1];
    }

    inline size_t size_at(size_t i) const {
        return offsets[i] - offsets[i - 1];
    }

    size_t size() const override {
        return offsets.size();
    }

    void clear() override {
        offsets.clear();
        chars.clear();
    }

    void insert_from(const IColumn& src, size_t n) override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override;

    int compare_at(size_t n, size_t m, const IColumn& rhs) const override;


protected:
    Offsets offsets;
    Chars chars;
};

}
