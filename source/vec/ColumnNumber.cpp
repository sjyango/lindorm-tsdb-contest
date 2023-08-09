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

#include "vec/columns/ColumnNumber.h"
#include "vec/columns/ColumnFactory.h"

namespace LindormContest::vectorized {

template <typename T>
bool ColumnNumber<T>::operator==(const ColumnNumber<T>& rhs) const {
    if (size() != rhs.size()) {
        return false;
    }

    for (int i = 0; i < size(); ++i) {
        if (get(i) != rhs.get(i)) {
            return false;
        }
    }

    return true;
}

template <typename T>
bool ColumnNumber<T>::operator!=(const ColumnNumber<T>& rhs) const {
    return !(*this == rhs);
}

template <typename T>
void ColumnNumber<T>::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const ColumnNumber& src_vec = static_cast<const ColumnNumber&>(src);
    if (start + length > src_vec._data.size()) {
        std::cerr << "Parameters start = "<< start << ", length = " << length << ", are out of bound in ColumnNumber<T>::insert_range_from method (_data.size() = "<< src_vec._data.size() << ")." << std::endl;
    }
    size_t old_size = _data.size();
    _data.resize(old_size + length);
    memcpy(_data.data() + old_size, &src_vec._data[start], length * sizeof(T));
}

template <typename T>
void ColumnNumber<T>::insert_indices_from(const IColumn& src, const size_t* indices_begin, const size_t* indices_end) {
    if (indices_begin == indices_end) {
        return;
    }
    size_t origin_size = size();
    size_t new_size = indices_end - indices_begin;
    _data.resize(origin_size + new_size);
    const ColumnNumber<T>& src_data = reinterpret_cast<const ColumnNumber<T>&>(src);

    for (int i = 0; i < new_size; ++i) {
        _data[origin_size + i] = src_data[indices_begin[i]];
    }
}

template <typename T>
int ColumnNumber<T>::compare_at(size_t n, size_t m, const IColumn& rhs_) const {
    const ColumnNumber& rhs = static_cast<const ColumnNumber&>(rhs_);
    return _data[n] > rhs.get_data()[m] ? 1 : (_data[n] < rhs.get_data()[m] ? -1 : 0);
}

template <typename T>
MutableColumnSPtr ColumnNumber<T>::clone_resized(size_t to_size) const {
    MutableColumnSPtr new_column = ColumnFactory::instance().create_column(get_type(), get_name());
    if (to_size == 0) {
        return new_column;
    }
    std::shared_ptr<ColumnNumber<T>> res = std::dynamic_pointer_cast<ColumnNumber<T>>(new_column);
    res->_data.resize(to_size);
    size_t count = std::min(size(), to_size);
    std::memcpy(res->_data.data(), _data.data(), count * sizeof(T));
    if (to_size > count) {
        std::memset(&res->_data[count], 0, (to_size - count) * sizeof(T));
    }
    return res;
}

template <typename T>
void ColumnNumber<T>::insert_many_data(const uint8_t* p, size_t num) {
    _data.insert(_data.end(), reinterpret_cast<const T*>(p), reinterpret_cast<const T*>(p) + num);
}

template <typename T>
void ColumnNumber<T>::insert_binary_data(const char* data, const uint32_t* offsets, const size_t num) {
    INFO_LOG("%s invokes insert_binary_data", typeid(*this).name())
    throw std::runtime_error("ColumnNumber doesn't implement insert_binary_data");
}

}
