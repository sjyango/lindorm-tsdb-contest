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

#include "IColumn.h"
#include "Root.h"

namespace LindormContest::vectorized {

class ColumnString : public IColumn {
public:
    using Char = UInt8;
    using Chars = std::vector<Char>;

    ColumnString(String column_name) : IColumn(column_name) {}

    ColumnString(const ColumnString& src)
            : IColumn(src.get_name()), _offsets(src._offsets.begin(), src._offsets.end()),
              _chars(src._chars.begin(), src._chars.end()) {}

    ~ColumnString() override = default;

    ColumnType get_type() const {
        return ColumnType::COLUMN_TYPE_STRING;
    }

    inline size_t offset_at(size_t i) const {
        return _offsets[i - 1];
    }

    inline size_t size_at(size_t i) const {
        return _offsets[i] - _offsets[i - 1];
    }

    String operator[](size_t n) {
        return {reinterpret_cast<const char*>(_chars.data() + offset_at(n)), size_at(n)};
    }

    size_t size() const override {
        return _offsets.size();
    }

    Chars& get_chars() { return _chars; }

    const Chars& get_chars() const { return _chars; }

    Offsets& get_offsets() { return _offsets; }

    const Offsets& get_offsets() const { return _offsets; }

    void clear() override {
        _offsets.clear();
        _chars.clear();
    }

    void push_string(const char* pos, size_t length) {
        const size_t old_size = _chars.size();
        const size_t new_size = old_size + length;

        if (length) {
            _chars.resize(new_size);
            std::memcpy(_chars.data() + old_size, pos, length);
        }
        _offsets.push_back(new_size);
    }

    void insert_from(const IColumn& src, size_t n) override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override;

    int compare_at(size_t n, size_t m, const IColumn& rhs) const override;

    MutableColumnSPtr clone_resized(size_t s) const override;

protected:
    Offsets _offsets;
    Chars _chars;
};

}
