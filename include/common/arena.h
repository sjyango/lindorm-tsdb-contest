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

namespace LindormContest::storage {

static constexpr size_t CHUNK_PAGE_SIZE = 4096;

class Arena {
public:
    Arena(size_t initial_size = CHUNK_PAGE_SIZE, size_t growth_factor = 2,
          size_t linear_growth_threshold = 128 * 1024 * 1024)
            : _growth_factor(growth_factor),
              _linear_growth_threshold(linear_growth_threshold),
              _head(new Chunk(initial_size, nullptr)),
              _size_in_bytes(_head->size()) {}

    ~Arena() { delete _head; }

    /// Get piece of memory, without alignment.
    char* alloc(size_t size) {
        if (_head->_pos + size > _head->_end) {
            add_chunk(size);
        }

        char* res = _head->_pos;
        _head->_pos += size;
        return res;
    }

    /// Get piece of memory with alignment
    char* aligned_alloc(size_t size, size_t alignment) {
        do {
            void* head_pos = _head->_pos;
            size_t space = _head->remaining();

            auto res = static_cast<char*>(std::align(alignment, size, head_pos, space));

            if (res) {
                _head->_pos = static_cast<char*>(head_pos);
                _head->_pos += size;
                return res;
            }

            add_chunk(size + alignment);
        } while (true);
    }

    template <typename T>
    T* alloc() {
        return reinterpret_cast<T*>(aligned_alloc(sizeof(T), alignof(T)));
    }

    void* rollback(size_t size) {
        _head->_pos -= size;
        return _head->_pos;
    }

    char* alloc_continue(size_t additional_bytes, char const*& range_start,
                         size_t start_alignment = 0) {
        if (!range_start) {
            // Start a new memory range.
            char* result = start_alignment ? aligned_alloc(additional_bytes, start_alignment)
                                           : alloc(additional_bytes);

            range_start = result;
            return result;
        }

        // Extend an existing memory range with 'additional_bytes'.

        // This method only works for extending the last allocation. For lack of
        // original size, check a weaker condition: that 'begin' is at least in
        // the current Chunk.
        assert(range_start >= _head->_begin && range_start < _head->_end);

        if (_head->_pos + additional_bytes <= _head->_end) {
            // The new size fits into the last chunk, so just alloc the
            // additional size. We can alloc without alignment here, because it
            // only applies to the start of the range, and we don't change it.
            return alloc(additional_bytes);
        }

        // New range doesn't fit into this chunk, will copy to a new one.
        //
        // Note: among other things, this method is used to provide a hack-ish
        // implementation of realloc over Arenas in ArenaAllocators. It wastes a
        // lot of memory -- quadratically so when we reach the linear allocation
        // threshold. This deficiency is intentionally left as is, and should be
        // solved not by complicating this method, but by rethinking the
        // approach to memory management for aggregate function states, so that
        // we can provide a proper realloc().
        const size_t existing_bytes = _head->_pos - range_start;
        const size_t new_bytes = existing_bytes + additional_bytes;
        const char* old_range = range_start;

        char* new_range =
                start_alignment ? aligned_alloc(new_bytes, start_alignment) : alloc(new_bytes);

        memcpy(new_range, old_range, existing_bytes);

        range_start = new_range;
        return new_range + existing_bytes;
    }

    /// NOTE: old memory region is wasted.
    char* realloc(const char* old_data, size_t old_size, size_t new_size) {
        char* res = alloc(new_size);
        if (old_data) {
            memcpy(res, old_data, old_size);
        }
        return res;
    }

    /// NOTE: old memory region is wasted.
    char* aligned_realloc(const char* old_data, size_t old_size, size_t new_size,
                          size_t alignment) {
        char* res = aligned_alloc(new_size, alignment);
        if (old_data) {
            memcpy(res, old_data, old_size);
        }
        return res;
    }

    /// Insert string without alignment.
    const char* insert(const char* data, size_t size) {
        char* res = alloc(size);
        memcpy(res, data, size);
        return res;
    }

    const char* aligned_insert(const char* data, size_t size, size_t alignment) {
        char* res = aligned_alloc(size, alignment);
        memcpy(res, data, size);
        return res;
    }

    size_t size() const { return _size_in_bytes; }

    size_t remaining_space_in_current_chunk() const { return _head->remaining(); }

private:
    struct Chunk {
        char* _begin;
        char* _pos;
        char* _end;

        Chunk* _prev;

        Chunk(size_t size, Chunk* prev) {
            _begin = reinterpret_cast<char*>(malloc(size));
            _pos = _begin;
            _end = _begin + size;
            _prev = prev;
        }

        ~Chunk() {
            free(_begin);

            if (_prev) {
                delete _prev;
            }
        }

        size_t size() const { return _end - _begin; }
        size_t remaining() const { return _end - _pos; }
    };

    size_t _growth_factor;
    size_t _linear_growth_threshold;

    Chunk* _head;
    size_t _size_in_bytes;

    static inline size_t round_up_to_page_size(size_t s) {
        return (s + CHUNK_PAGE_SIZE - 1) / CHUNK_PAGE_SIZE * CHUNK_PAGE_SIZE;
    }

    /// If chunks size is less than 'linear_growth_threshold', then use exponential growth, otherwise - linear growth
    ///  (to not allocate too much excessive memory).
    size_t next_size(size_t min_next_size) const {
        size_t size_after_grow = 0;

        if (_head->size() < _linear_growth_threshold) {
            size_after_grow = std::max(min_next_size, _head->size() * _growth_factor);
        } else {
            // alloc_continue() combined with linear growth results in quadratic
            // behavior: we append the data by small amounts, and when it
            // doesn't fit, we create a new chunk and copy all the previous data
            // into it. The number of times we do this is directly proportional
            // to the total size of data that is going to be serialized. To make
            // the copying happen less often, round the next size up to the
            // linear_growth_threshold.
            size_after_grow =
                    ((min_next_size + _linear_growth_threshold - 1) / _linear_growth_threshold) *
                    _linear_growth_threshold;
        }

        assert(size_after_grow >= min_next_size);
        return round_up_to_page_size(size_after_grow);
    }

    void add_chunk(size_t min_size) {
        _head = new Chunk(next_size(min_size), _head);
        _size_in_bytes += _head->size();
    }
};

}