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

#include "common/slice.h"
#include "io/io_utils.h"
#include "storage/segment_traits.h"

namespace LindormContest::storage {

// 如果一个缓存节点的in cache为true，并且refs等于1，则放置在lru中
// 如果一个缓存节点的in cache为true，并且refs大于等于2，则放置在in use中
// 如果一个缓存节点的in cache为false，则表明该节点未缓存在内存中
struct LRUHandle {
    using Key = io::PagePointer;
    using Value = std::shared_ptr<DataPage>;

    Key _key;
    Value _value;
    LRUHandle* _next_hash;
    LRUHandle* _next;
    LRUHandle* _prev;
    bool _in_cache;
    uint32_t _refs;
    uint32_t _hash;
};

class HandleHashTable {
public:
    HandleHashTable() : _length(0), _elems(0), _list(nullptr) {
        _resize();
    }

    ~HandleHashTable() {
        delete[] _list;
    }

    LRUHandle* lookup(const LRUHandle::Key& key, uint32_t hash) {
        return *_find_pointer(key, hash);
    }

    LRUHandle* insert(LRUHandle* h) {
        LRUHandle** ptr = _find_pointer(h->_key, h->_hash);
        LRUHandle* old = *ptr;
        h->_next_hash = (old == nullptr ? nullptr : old->_next_hash);
        *ptr = h;
        if (old == nullptr) {
            ++_elems;
            if (_elems > _length) {
                // Since each cache entry is fairly large, we aim for a small
                // average linked list length (<= 1).
                _resize();
            }
        }
        return old;
    }

    LRUHandle* remove(const LRUHandle::Key& key, uint32_t hash) {
        LRUHandle** ptr = _find_pointer(key, hash);
        LRUHandle* result = *ptr;
        if (result != nullptr) {
            *ptr = result->_next_hash;
            --_elems;
        }
        return result;
    }

private:
    // The table consists of an array of buckets where each bucket is
    // a linked list of cache entries that hash into the bucket.
    uint32_t _length;   // hashtable桶的个数
    uint32_t _elems;    // hashtable中元素的个数
    LRUHandle** _list;  // 构造一个数组，数组中的每个元素为LRUHandle*

    // Return a pointer to slot that points to a cache entry that
    // matches key/hash.  If there is no such cache entry, return a
    // pointer to the trailing slot in the corresponding linked list.
    LRUHandle** _find_pointer(const LRUHandle::Key& key, uint32_t hash) {
        LRUHandle** ptr = &_list[hash & (_length - 1)];

        while (*ptr != nullptr && ((*ptr)->_hash != hash || key != (*ptr)->_key)) {
            ptr = &(*ptr)->_next_hash;
        }

        return ptr;
    }

    void _resize() {
        uint32_t new_length = 4;

        while (new_length < _elems) {
            new_length *= 2;
        }

        LRUHandle** new_list = new LRUHandle*[new_length];
        std::memset(new_list, 0, sizeof(new_list[0]) * new_length);
        uint32_t count = 0;

        for (uint32_t i = 0; i < _length; ++i) {
            LRUHandle* h = _list[i];
            while (h != nullptr) {
                LRUHandle* next = h->_next_hash;
                uint32_t hash = h->_hash;
                LRUHandle** ptr = &new_list[hash & (new_length - 1)];
                h->_next_hash = *ptr;
                *ptr = h;
                h = next;
                count++;
            }
        }

        assert(_elems == count);
        delete[] _list;
        _list = new_list;
        _length = new_length;
    }
};

}