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
#include "IColumn.h"

namespace LindormContest::vectorized {

template <typename T>
class ColumnNumber : public IColumn {
public:
    using Container = std::vector<T>;

    ColumnNumber(String column_name)
            : IColumn(column_name) {}

    ColumnNumber(String column_name, const size_t n)
            : IColumn(column_name), data(n) {}

    ColumnNumber(String column_name, const size_t n, const T x)
            : IColumn(column_name), data(n, x) {}

    ColumnNumber(String column_name, std::initializer_list<T> il)
            : IColumn(column_name), data{il} {}

    ColumnNumber(String column_name, const ColumnNumber& src)
            : IColumn(src.get_name()), data(src.data.begin(), src.data.end()) {}

    ~ColumnNumber() override = default;

    ColumnType get_type() const override {
        if constexpr (std::is_same_v<T, Int32>) {
            return ColumnType::COLUMN_TYPE_INTEGER;
        } else if constexpr (std::is_same_v<T, Float64>) {
            return ColumnType::COLUMN_TYPE_DOUBLE_FLOAT;
        } else {
            return ColumnType::COLUMN_TYPE_UNINITIALIZED;
        }
    }

    const Container& get_data() const {
        return data;
    }

    size_t size() const override {
        return data.size();
    }

    void clear() override {
        data.clear();
    }

    std::string_view get_string_view() const override {
        return std::string_view {reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T)};
    }

    void push_number(T val);

    void insert_from(const IColumn& src, size_t n) override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override;

    int compare_at(size_t n, size_t m, const IColumn& rhs) const override;

    MutableColumnPtr clone_resized(size_t s) const override;

private:
    Container data;
};

using ColumnInt32 = ColumnNumber<Int32>;
using ColumnInt64 = ColumnNumber<Int64>;
using ColumnFloat64 = ColumnNumber<Float64>;

}
