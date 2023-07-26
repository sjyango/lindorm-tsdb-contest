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

namespace LindormContest::vectorized {

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
void ColumnNumber<T>::insert_indices_from(const IColumn& src, const int* indices_begin,
                         const int* indices_end) {
    size_t origin_size = size();
    size_t new_size = indices_end - indices_begin;
    _data.resize(origin_size + new_size);

    const T* src__data = reinterpret_cast<const T*>(src.get_string_view().data());

    for (int i = 0; i < new_size; ++i) {
        _data[origin_size + i] = src__data[indices_begin[i]];
    }
}

template <typename T>
int ColumnNumber<T>::compare_at(size_t n, size_t m, const IColumn& rhs_) const {
    const ColumnNumber& rhs = static_cast<const ColumnNumber&>(rhs_);
    return _data[n] > rhs.get_data()[m] ? 1 : (_data[n] < rhs.get_data()[m] ? -1 : 0);
}

template <typename T>
MutableColumnPtr ColumnNumber<T>::clone_resized(size_t to_size) const {
    auto res = new ColumnNumber<T>(get_name());
    if (to_size == 0) {
        return res;
    }
    res->_data.resize(to_size);
    size_t count = std::min(size(), to_size);
    std::memcpy(res->_data.data(), _data.data(), count * sizeof(T));
    if (to_size > count) {
        std::memset(&res->_data[count], 0, (to_size - count) * sizeof(T));
    }
    return res;
}

}
