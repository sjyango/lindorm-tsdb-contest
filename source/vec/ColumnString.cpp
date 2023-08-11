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

#include "vec/columns/ColumnString.h"
#include "vec/columns/ColumnFactory.h"

namespace LindormContest::vectorized {

bool ColumnString::operator==(const ColumnString& rhs) const {
    if (size() != rhs.size()) {
        return false;
    }

    for (size_t i = 0; i < size(); ++i) {
        if (get(i) != rhs.get(i)) {
            return false;
        }
    }

    return true;
}

bool ColumnString::operator!=(const ColumnString& rhs) const {
    return !(*this == rhs);
}

void ColumnString::insert_from(const IColumn& src, size_t n) {
    const ColumnString& src_vec = static_cast<const ColumnString&>(src);
    const size_t size_to_append = n != 0 ? src_vec._offsets[n] - src_vec._offsets[n - 1] : src_vec._offsets[0];

    if (size_to_append == 0) {
        // insert empty string
        _offsets.push_back(_chars.size());
    } else {
        const size_t old_size = _chars.size();
        const size_t offset = src_vec.offset_at(n);
        const size_t new_size = old_size + size_to_append;

        _chars.resize(new_size);
        memcpy(_chars.data() + old_size, &src_vec._chars[offset], size_to_append);
        _offsets.push_back(new_size);
    }
}

void ColumnString::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) {
        return;
    }

    const ColumnString& src_vec = static_cast<const ColumnString&>(src);

    if (start + length > src_vec._offsets.size()) {
        std::cerr << "Parameter out of bound in IColumnString::insert_range_from method.";
    }

    size_t src_offset = src_vec.offset_at(start);
    size_t src_length = src_vec.offset_at(start + length) - src_offset;

    size_t old_size = _chars.size();
    _chars.resize(old_size + src_length);
    std::memcpy(&_chars[old_size], &src_vec._chars[src_offset], src_length);

    if (start == 0 && _offsets.empty()) {
        _offsets.assign(src_vec._offsets.begin(), src_vec._offsets.begin() + length);
    } else {
        size_t old_offset_size = _offsets.size();
        size_t old_max_offset = _offsets.back();
        _offsets.resize(old_offset_size + length);

        for (size_t i = 0; i < length; ++i) {
            _offsets[old_offset_size + i] = src_vec._offsets[start + i] - src_offset + old_max_offset;
        }
    }
}

void ColumnString::insert_indices_from(const IColumn& src, const size_t* indices_begin, const size_t* indices_end) {
    if (indices_begin == indices_end) {
        return;
    }
    size_t new_size = indices_end - indices_begin;
    const ColumnString& src_vec = static_cast<const ColumnString&>(src);

    for (size_t i = 0; i < new_size; ++i) {
        push_string(src_vec[indices_begin[i]]);
    }
}

int ColumnString::compare_at(size_t n, size_t m, const IColumn& rhs_) const {
    const ColumnString& rhs = static_cast<const ColumnString&>(rhs_);
    size_t lhs_length = size_at(n);
    size_t rhs_length = rhs.size_at(m);
    size_t min_length = std::min(lhs_length, rhs_length);
    int res = std::memcmp(_chars.data() + offset_at(n), rhs._chars.data() + rhs.offset_at(m), min_length);
    if (res) {
        return res;
    }
    // res == 0
    if (lhs_length > rhs_length) {
        return 1;
    } else if (lhs_length == rhs_length) {
        return 0;
    } else {
        return -1;
    }
}

MutableColumnSPtr ColumnString::clone_resized(size_t to_size) const {
    MutableColumnSPtr new_column = ColumnFactory::instance().create_column(get_type(), get_name());
    if (to_size == 0) {
        return new_column;
    }
    std::shared_ptr<ColumnString> res = std::dynamic_pointer_cast<ColumnString>(new_column);
    size_t from_size = size();
    if (to_size <= from_size) {
        // just cut column
        res->_offsets.assign(_offsets.begin(), _offsets.begin() + to_size);
        res->_chars.assign(_chars.begin(), _chars.begin() + _offsets[to_size - 1]);
    } else {
        // copy column and append empty string for extra elements
        if (from_size > 0) {
            res->_offsets.assign(_offsets.begin(), _offsets.end());
            res->_chars.assign(_chars.begin(), _chars.end());
        }
        res->_offsets.resize(to_size, _chars.size());
    }
    return res;
}

void ColumnString::insert_binary_data(const char* data, const uint32_t* offsets, const size_t num) {
    if (num == 0) {
        return;
    }
    const auto old_size = _chars.size();
    const size_t total_mem_size = offsets[num - 1];
    if (total_mem_size > 0) {
        _chars.resize(total_mem_size + old_size);
        memcpy(_chars.data() + old_size, data, total_mem_size);
    }
    const auto old_rows = _offsets.size();
    auto tail_offset = old_rows == 0 ? 0 : _offsets.back();
    assert(tail_offset == old_size);
    _offsets.resize(old_rows + num);

    for (size_t i = 0; i < num; ++i) {
        _offsets[old_rows + i] = tail_offset + offsets[i];
    }
    assert(_chars.size() == _offsets.back());
}

void ColumnString::insert_many_data(const uint8_t* data, size_t num) {
    INFO_LOG("%s invokes insert_many_data", typeid(*this).name())
    throw std::runtime_error("ColumnString doesn't implement insert_many_data");
}

}
