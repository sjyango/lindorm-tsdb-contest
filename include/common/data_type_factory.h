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
#include "common/slice.h"
#include "struct/ColumnValue.h"

namespace LindormContest {

class DataType {
public:
    template <typename DataTypeTraits>
    DataType(DataTypeTraits)
            : _cmp(DataTypeTraits::cmp),
              _type_size(DataTypeTraits::TYPE_SIZE),
              _column_type(DataTypeTraits::COLUMN_TYPE) {}

    ~DataType() = default;

    int cmp(const void* lhs, const void* rhs) const {
        return _cmp(lhs, rhs);
    }

    const size_t type_size() const {
        return _type_size;
    }

    const ColumnType column_type() const {
        return _column_type;
    }

private:
    int (*_cmp)(const void* lhs, const void* rhs);

    const size_t _type_size;
    const ColumnType _column_type;
};

template <ColumnType type>
struct DataTypeTraits {};

template <>
struct DataTypeTraits<COLUMN_TYPE_STRING> {
    using CPP_TYPE = Slice;
    static constexpr ColumnType COLUMN_TYPE = COLUMN_TYPE_STRING;
    static constexpr size_t TYPE_SIZE = sizeof(CPP_TYPE);

    static inline CPP_TYPE get_value(const void* p) {
        return *reinterpret_cast<const CPP_TYPE*>(p);
    }

    static inline void set_value(void* p, const CPP_TYPE& value) {
        std::memcpy(p, &value, TYPE_SIZE);
    }

    static inline int cmp(const void* lhs, const void* rhs) {
        CPP_TYPE lhs_value = get_value(lhs);
        CPP_TYPE rhs_value = get_value(rhs);
        return lhs_value.compare(rhs_value);
    }
};

template <>
struct DataTypeTraits<COLUMN_TYPE_INTEGER> {
    using CPP_TYPE = int32_t;
    static constexpr ColumnType COLUMN_TYPE = COLUMN_TYPE_INTEGER;
    static constexpr size_t TYPE_SIZE = sizeof(CPP_TYPE);

    static inline CPP_TYPE get_value(const void* p) {
        return *reinterpret_cast<const CPP_TYPE*>(p);
    }

    static inline void set_value(void* p, const CPP_TYPE& value) {
        std::memcpy(p, &value, TYPE_SIZE);
    }

    static inline int cmp(const void* lhs, const void* rhs) {
        CPP_TYPE lhs_value = get_value(lhs);
        CPP_TYPE rhs_value = get_value(rhs);
        if (lhs_value < rhs_value) {
            return -1;
        } else if (lhs_value > rhs_value) {
            return 1;
        } else {
            return 0;
        }
    }
};

template <>
struct DataTypeTraits<COLUMN_TYPE_TIMESTAMP> {
    using CPP_TYPE = int64_t;
    static constexpr ColumnType COLUMN_TYPE = COLUMN_TYPE_TIMESTAMP;
    static constexpr size_t TYPE_SIZE = sizeof(CPP_TYPE);

    static inline CPP_TYPE get_value(const void* p) {
        return *reinterpret_cast<const CPP_TYPE*>(p);
    }

    static inline void set_value(void* p, const CPP_TYPE& value) {
        std::memcpy(p, &value, TYPE_SIZE);
    }

    static inline int cmp(const void* lhs, const void* rhs) {
        CPP_TYPE lhs_value = get_value(lhs);
        CPP_TYPE rhs_value = get_value(rhs);
        if (lhs_value < rhs_value) {
            return -1;
        } else if (lhs_value > rhs_value) {
            return 1;
        } else {
            return 0;
        }
    }
};

template <>
struct DataTypeTraits<COLUMN_TYPE_DOUBLE_FLOAT> {
    using CPP_TYPE = double_t;
    static constexpr ColumnType COLUMN_TYPE = COLUMN_TYPE_DOUBLE_FLOAT;
    static constexpr size_t TYPE_SIZE = sizeof(CPP_TYPE);

    static inline CPP_TYPE get_value(const void* p) {
        return *reinterpret_cast<const CPP_TYPE*>(p);
    }

    static inline void set_value(void* p, const CPP_TYPE& value) {
        std::memcpy(p, &value, TYPE_SIZE);
    }

    static inline int cmp(const void* lhs, const void* rhs) {
        CPP_TYPE lhs_value = get_value(lhs);
        CPP_TYPE rhs_value = get_value(rhs);
        if (lhs_value < rhs_value) {
            return -1;
        } else if (lhs_value > rhs_value) {
            return 1;
        } else {
            return 0;
        }
    }
};

template <ColumnType column_type>
const DataType* create_column_data_type() {
    static constexpr DataTypeTraits<column_type> traits;
    static DataType column_data_type(traits);
    return &column_data_type;
}

class DataTypeFactory {
public:
    static DataTypeFactory& instance() {
        static DataTypeFactory instance;
        return instance;
    }

    const DataType* get_column_data_type(ColumnType column_type) {
        // nullptr means that there is no column_data_type implementation for the corresponding column_type
        static const DataType* column_data_type_array[] = {
                nullptr,
                create_column_data_type<ColumnType::COLUMN_TYPE_STRING>(),
                create_column_data_type<ColumnType::COLUMN_TYPE_INTEGER>(),
                create_column_data_type<ColumnType::COLUMN_TYPE_DOUBLE_FLOAT>(),
                create_column_data_type<ColumnType::COLUMN_TYPE_TIMESTAMP>(),
                nullptr
        };
        return column_data_type_array[int(column_type)];
    }
};

}
