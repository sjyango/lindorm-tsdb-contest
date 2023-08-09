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
#include "vec/columns/IColumn.h"

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

    bool operator==(const ColumnString& rhs) const;

    bool operator!=(const ColumnString& rhs) const;

    ColumnType get_type() const {
        return ColumnType::COLUMN_TYPE_STRING;
    }

    inline size_t offset_at(size_t i) const {
        return _offsets[i - 1];
    }

    inline size_t size_at(size_t i) const {
        return _offsets[i] - _offsets[i - 1];
    }

    String get(size_t n) {
        return (*this)[n];
    }

    String get(size_t n) const {
        return (*this)[n];
    }

    String operator[](size_t n) {
        return {reinterpret_cast<const char*>(_chars.data() + offset_at(n)), size_at(n)};
    }

    String operator[](size_t n) const {
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

    void push_string(const String& str) {
        push_string(str.c_str(), str.size());
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

    void reserve(size_t n) override {
        _offsets.reserve(n);
        _chars.reserve(n);
    }

    void insert_from(const IColumn& src, size_t n) override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const size_t* indices_begin,
                             const size_t* indices_end) override;

    int compare_at(size_t n, size_t m, const IColumn& rhs) const override;

    MutableColumnSPtr clone_resized(size_t s) const override;

    void insert_binary_data(const char* data, const uint32_t* offsets, const size_t num) override;

    void insert_many_data(const uint8_t* data, size_t num) override;

protected:
    Offsets _offsets;
    Chars _chars;
};

}
