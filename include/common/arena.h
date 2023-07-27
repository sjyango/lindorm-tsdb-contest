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

namespace LindormContest {

static constexpr size_t BLOCK_SIZE = 4096;

class Arena {
public:
    Arena() : _alloc_ptr(nullptr), _alloc_bytes_remaining(0), _memory_usage(0) {}

    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    ~Arena() {
        for (size_t i = 0; i < _blocks.size(); i++) {
            delete[] _blocks[i];
        }
    }

    char* allocate(size_t bytes) {
        assert(bytes > 0);
        if (bytes <= _alloc_bytes_remaining) {
            char* result = _alloc_ptr;
            _alloc_ptr += bytes;
            _alloc_bytes_remaining -= bytes;
            return result;
        }
        return _allocate_fallback(bytes);
    }

    char* allocate_aligned(size_t bytes) {
        const int align = 8;
        size_t current_mod = reinterpret_cast<uintptr_t>(_alloc_ptr) & (align - 1);
        size_t slop = (current_mod == 0 ? 0 : align - current_mod);
        size_t needed = bytes + slop;
        char* result;
        if (needed <= _alloc_bytes_remaining) {
            result = _alloc_ptr + slop;
            _alloc_ptr += needed;
            _alloc_bytes_remaining -= needed;
        } else {
            result = _allocate_fallback(bytes);
        }
        assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
        return result;
    }

    size_t memory_usage() const {
        return _memory_usage.load(std::memory_order_relaxed);
    }

private:
    char* _allocate_fallback(size_t bytes) {
        if (bytes > BLOCK_SIZE / 4) {
            char* result = _allocate_new_block(bytes);
            return result;
        }
        // We waste the remaining space in the current block.
        _alloc_ptr = _allocate_new_block(BLOCK_SIZE);
        _alloc_bytes_remaining = BLOCK_SIZE;
        char* result = _alloc_ptr;
        _alloc_ptr += bytes;
        _alloc_bytes_remaining -= bytes;
        return result;
    }

    char* _allocate_new_block(size_t block_bytes) {
        char* result = new char[block_bytes];
        _blocks.push_back(result);
        _memory_usage.fetch_add(block_bytes + sizeof(char*), std::memory_order_relaxed);
        return result;
    }

    char* _alloc_ptr;
    size_t _alloc_bytes_remaining;
    std::vector<char*> _blocks;
    std::atomic<size_t> _memory_usage;
};

}