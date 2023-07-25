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

namespace LindormContest::vectorized {

void ColumnString::push_string(const char* pos, size_t length) {
    const size_t old_size = chars.size();
    const size_t new_size = old_size + length;

    if (length) {
        chars.resize(new_size);
        std::memcpy(chars.data() + old_size, pos, length);
    }
    offsets.push_back(new_size);
}

void ColumnString::insert_from(const IColumn& src, size_t n) {
    const ColumnString& src_vec = static_cast<const ColumnString&>(src);
    const size_t size_to_append = src_vec.offsets[n] - src_vec.offsets[n - 1];

    if (!size_to_append) {
        // insert empty string
        offsets.push_back(chars.size());
    } else {
        const size_t old_size = chars.size();
        const size_t offset = src_vec.offsets[n - 1];
        const size_t new_size = old_size + size_to_append;

        chars.resize(new_size);
        memcpy(chars.data() + old_size, &src_vec.chars[offset], size_to_append);
        offsets.push_back(new_size);
    }
}

void ColumnString::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) {
        return;
    }

    const ColumnString& src_vec = static_cast<const ColumnString&>(src);

    if (start + length > src_vec.offsets.size()) {
        std::cerr << "Parameter out of bound in IColumnString::insert_range_from method.";
    }

    size_t nested_offset = src_vec.offset_at(start);
    size_t nested_length = src_vec.offsets[start + length - 1] - nested_offset;

    size_t old_chars_size = chars.size();
    chars.resize(old_chars_size + nested_length);
    memcpy(&chars[old_chars_size], &src_vec.chars[nested_offset], nested_length);

    if (start == 0 && offsets.empty()) {
        offsets.assign(src_vec.offsets.begin(), src_vec.offsets.begin() + length);
    } else {
        size_t old_size = offsets.size();
        size_t prev_max_offset = offsets.back();
        offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i) {
            offsets[old_size + i] = src_vec.offsets[start + i] - nested_offset + prev_max_offset;
        }
    }
}

void ColumnString::insert_indices_from(const IColumn& src, const int* indices_begin,
                                          const int* indices_end) {
    const ColumnString& src_str = static_cast<const ColumnString&>(src);
    auto src_offset_data = src_str.offsets.data();

    auto old_char_size = chars.size();
    size_t total_chars_size = old_char_size;

    auto dst_offsets_pos = offsets.size();
    offsets.resize(offsets.size() + indices_end - indices_begin);
    auto* dst_offsets_data = offsets.data();

    for (auto x = indices_begin; x != indices_end; ++x) {
        if (*x != -1) {
            total_chars_size += src_offset_data[*x] - src_offset_data[*x - 1];
        }
        dst_offsets_data[dst_offsets_pos++] = total_chars_size;
    }

    chars.resize(total_chars_size);

    auto* src_data_ptr = src_str.chars.data();
    auto* dst_data_ptr = chars.data();

    size_t dst_chars_pos = old_char_size;

    for (auto x = indices_begin; x != indices_end; ++x) {
        if (*x != -1) {
            const size_t size_to_append = src_offset_data[*x] - src_offset_data[*x - 1];
            const size_t offset = src_offset_data[*x - 1];
            memcpy(dst_data_ptr + dst_chars_pos, src_data_ptr + offset, size_to_append);
            dst_chars_pos += size_to_append;
        }
    }
}

int ColumnString::compare_at(size_t n, size_t m, const IColumn& rhs_) const {
    const ColumnString& rhs = static_cast<const ColumnString&>(rhs_);
    size_t lhs_length = size_at(n);
    size_t rhs_length = rhs.size_at(m);
    size_t min_length = std::min(lhs_length, rhs_length);
    int res = memcmp(chars.data() + offset_at(n), rhs.chars.data() + rhs.offset_at(m), min_length);
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

MutableColumnPtr ColumnString::clone_resized(size_t to_size) const {
    auto res = new ColumnString(get_name());
    if (to_size == 0) {
        return res;
    }
    size_t from_size = size();
    if (to_size <= from_size) {
        // just cut column
        res->offsets.assign(offsets.begin(), offsets.begin() + to_size);
        res->chars.assign(chars.begin(), chars.begin() + offsets[to_size - 1]);
    } else {
        // copy column and append empty string for extra elements
        if (from_size > 0) {
            res->offsets.assign(offsets.begin(), offsets.end());
            res->chars.assign(chars.begin(), chars.end());
        }
        res->offsets.resize(to_size, chars.size());
    }
    return res;
}

}
