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

#include "vec/ColumnNumber.h"

namespace LindormContest::vectorized {

template <typename T>
void ColumnNumber<T>::insert_from(const IColumn& src, size_t n) {
    data.push_back(static_cast<const ColumnNumber&>(src).data[n]);
}

template <typename T>
void ColumnNumber<T>::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const ColumnNumber& src_vec = static_cast<const ColumnNumber&>(src);
    if (start + length > src_vec.data.size()) {
        std::cerr << "Parameters start = "<< start << ", length = " << length << ", are out of bound in ColumnNumber<T>::insert_range_from method (data.size() = "<< src_vec.data.size() << ")." << std::endl;
    }
    size_t old_size = data.size();
    data.resize(old_size + length);
    memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(T));
}

template <typename T>
void ColumnNumber<T>::insert_indices_from(const IColumn& src, const int* indices_begin,
                         const int* indices_end) {
    size_t origin_size = size();
    size_t new_size = indices_end - indices_begin;
    data.resize(origin_size + new_size);

    const T* src_data = reinterpret_cast<const T*>(src.get_string_view().data());

    for (int i = 0; i < new_size; ++i) {
        data[origin_size + i] = src_data[indices_begin[i]];
    }
}

template <typename T>
int ColumnNumber<T>::compare_at(size_t n, size_t m, const IColumn& rhs_) const {
    const ColumnNumber& rhs = static_cast<const ColumnNumber&>(rhs_);
    return data[n] > rhs.get_data()[m] ? 1 : (data[n] < rhs.get_data()[m] ? -1 : 0);
}

}
