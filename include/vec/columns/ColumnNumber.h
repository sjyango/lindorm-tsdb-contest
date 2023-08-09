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

template <typename T>
class ColumnNumber : public IColumn {
public:
    using Container = std::vector<T>;

    ColumnNumber(String column_name)
            : IColumn(column_name) {}

    ColumnNumber(String column_name, const size_t n)
            : IColumn(column_name), _data(n) {}

    ColumnNumber(String column_name, const size_t n, const T x)
            : IColumn(column_name), _data(n, x) {}

    ColumnNumber(String column_name, std::initializer_list<T> il)
            : IColumn(column_name), _data{il} {}

    ColumnNumber(String column_name, const ColumnNumber& src)
            : IColumn(src.get_name()), _data(src._data.begin(), src._data.end()) {}

    ~ColumnNumber() override = default;

    bool operator==(const ColumnNumber<T>& rhs) const;

    bool operator!=(const ColumnNumber<T>& rhs) const;

    ColumnType get_type() const override {
        if constexpr (std::is_same_v<T, Int32>) {
            return ColumnType::COLUMN_TYPE_INTEGER;
        } else if constexpr (std::is_same_v<T, Int64>) {
            return ColumnType::COLUMN_TYPE_TIMESTAMP;
        } else if constexpr (std::is_same_v<T, Float64>) {
            return ColumnType::COLUMN_TYPE_DOUBLE_FLOAT;
        } else {
            return ColumnType::COLUMN_TYPE_UNINITIALIZED;
        }
    }

    Container& get_data() {
        return _data;
    }

    const Container& get_data() const {
        return _data;
    }

    T operator[](size_t n) {
        return get(n);
    }

    T operator[](size_t n) const {
        return get(n);
    }

    T get(size_t n) {
        return _data[n];
    }

    T get(size_t n) const {
        return _data[n];
    }

    size_t size() const override {
        return _data.size();
    }

    void clear() override {
        _data.clear();
    }

    std::string_view get_string_view() const override {
        return std::string_view {reinterpret_cast<const char*>(_data.data()), _data.size() * sizeof(T)};
    }

    void push_number(T val) {
        _data.push_back(val);
    }

    void insert_from(const IColumn& src, size_t n) override {
        _data.push_back(static_cast<const ColumnNumber&>(src)._data[n]);
    }

    void reserve(size_t n) override {
        _data.reserve(n);
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const size_t* indices_begin, const size_t* indices_end) override;

    int compare_at(size_t n, size_t m, const IColumn& rhs_) const override;

    MutableColumnSPtr clone_resized(size_t to_size) const override;

    void insert_many_data(const uint8_t* data, size_t num) override;

    void insert_binary_data(const char* data, const uint32_t* offsets, const size_t num) override;

private:
    Container _data;
};

/// Explicit template instantiations - to avoid code bloat in headers.
template class ColumnNumber<Int32>;
template class ColumnNumber<Int64>;
template class ColumnNumber<Float64>;

using ColumnInt32 = ColumnNumber<Int32>;
using ColumnInt64 = ColumnNumber<Int64>;
using ColumnFloat64 = ColumnNumber<Float64>;

}
