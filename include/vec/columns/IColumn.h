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
#include "struct/ColumnValue.h"

namespace LindormContest::vectorized {

class IColumn;
class ColumnString;

using ColumnSPtr = std::shared_ptr<const IColumn>;
using MutableColumnSPtr = std::shared_ptr<IColumn>;
using SColumns = std::vector<ColumnSPtr>;
using SMutableColumns = std::vector<MutableColumnSPtr>;

using ColumnPtr = const IColumn*;
using MutableColumnPtr = IColumn*;
using Columns = std::vector<ColumnPtr>;
using MutableColumns = std::vector<MutableColumnPtr>;

class IColumn {
public:
    IColumn(String column_name) : _column_name(std::move(column_name)) {}

    IColumn(const IColumn&) = default;

    virtual ~IColumn() = default;

    std::string get_name() const {
        return _column_name;
    }

    virtual ColumnType get_type() const = 0;

    virtual size_t size() const = 0;

    bool empty() const { return size() == 0; }

    virtual std::string_view get_string_view() const {
        return std::string_view {};
    }

    virtual void clear() = 0;

    virtual size_t memory_usage() const = 0;

    /// Appends n-th element from other column with the same type.
    /// Is used in merge-sort and merges. It could be implemented in inherited classes more optimally than default implementation.
    virtual void insert_from(const IColumn& src, size_t n) = 0;

    /// Appends range of elements from other column with the same type.
    /// Could be used to concatenate columns.
    virtual void insert_range_from(const IColumn& src, size_t start, size_t length) = 0;

    /// Appends a batch elements from other column with the same type
    /// indices_begin + indices_end represent the row indices of column src
    /// Warning:
    ///       if *indices == -1 means the row is null, only use in outer join, do not use in any other place
    virtual void insert_indices_from(const IColumn& src, const size_t* indices_begin,
                                     const size_t* indices_end) = 0;

    virtual int compare_at(size_t n, size_t m, const IColumn& rhs) const = 0;

    virtual MutableColumnSPtr clone_resized(size_t s) const = 0;

    virtual MutableColumnSPtr clone_empty() const { return clone_resized(0); }

    virtual void reserve(size_t n) = 0;

    // just for ColumnString to implement
    virtual void insert_binary_data(const char* data, const uint32_t* offsets, const size_t num) = 0;

    // just for ColumnNumber to implement
    virtual void insert_many_data(const uint8_t* data, size_t num) = 0;

    static MutableColumnSPtr assume_mutable(ColumnSPtr column) {
        return std::const_pointer_cast<IColumn>(column);
    }

    using Offset = UInt32;
    using Offsets = std::vector<Offset>;

protected:
    std::string _column_name;
};

}
