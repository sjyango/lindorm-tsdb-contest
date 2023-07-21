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

template <typename T>
class ColumnNumber : public IColumn {
public:
    using Container = std::vector<T>;

    ColumnNumber() = default;

    ColumnNumber(const size_t n) : data(n) {}

    ColumnNumber(const size_t n, const T x) : data(n, x) {}

    ColumnNumber(std::initializer_list<T> il) : data{il} {}

    ColumnNumber(const ColumnNumber& src)
            : IColumn(src.get_name()), data(src.data.begin(), src.data.end()) {}

    ColumnType get_type() const override {
        if constexpr (std::is_same_v<T, Int32>) {
            return ColumnType::COLUMN_TYPE_INTEGER;
        } else if constexpr (std::is_same_v<T, Float64>) {
            return ColumnType::COLUMN_TYPE_DOUBLE_FLOAT;
        } else {
            return ColumnType::COLUMN_TYPE_UNINITIALIZED;
        }
    }

    size_t size() const override {
        return data.size();
    }

    std::string_view get_string_view() const override {
        return std::string_view {reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T)};
    }

    void insert_from(const IColumn& src, size_t n) override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const UInt32* indices_begin,
                             const UInt32* indices_end) override;

protected:
    Container data;
};

}
