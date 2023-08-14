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

#include <mutex>

#include "io/io_utils.h"
#include "hash_table.h"

namespace LindormContest::storage {

// A single shard of sharded cache.
class LRUCache {
public:
    LRUCache() : _capacity(0), _usage(0) {
        _lru._next = &_lru;
        _lru._prev = &_lru;
        _in_use._next = &_in_use;
        _in_use._prev = &_in_use;
    }

    ~LRUCache() {
        assert(_in_use._next == &_in_use);
        // Error if caller has an unreleased handle
        for (LRUHandle* e = _lru._next; e != &_lru;) {
            LRUHandle* next = e->_next;
            assert(e->_in_cache);
            e->_in_cache = false;
            assert(e->_refs == 1);  // Invariant of lru_ list.
            _unref(e);
            e = next;
        }
    }

    void set_capacity(size_t capacity) {
        _capacity = capacity;
    }

    LRUHandle* insert(const LRUHandle::Key& key, LRUHandle::Value value, uint32_t hash) {
        LRUHandle* e = new LRUHandle();
        e->_key = key;
        e->_value = value;
        e->_hash = hash;
        e->_in_cache = false;
        e->_refs = 1;  // for the returned handle.

        if (_capacity > 0) {
            e->_refs++;  // for the cache's reference.
            e->_in_cache = true;
            _lru_append(&_in_use, e);
            _usage += 1;
            _finish_erase(_table.insert(e));
        } else {
            // don't cache. (capacity_==0 is supported and turns off caching.)
            // next is read by key() in an assert, so it must be initialized
            e->_next = nullptr;
        }

        while (_usage > _capacity && _lru._next != &_lru) {
            LRUHandle* old = _lru._next;
            assert(old->_refs == 1);
            bool erased = _finish_erase(_table.remove(old->_key, old->_hash));
            if (!erased) {
                // to avoid unused variable when compiled NDEBUG
                assert(erased);
            }
        }

        return e;
    }

    LRUHandle* lookup(const LRUHandle::Key& key, uint32_t hash) {
        LRUHandle* e = _table.lookup(key, hash);
        if (e != nullptr) {
            _ref(e);
        }
        return e;
    }

    void release(LRUHandle* e) {
        _unref(e);
    }

    void erase(const LRUHandle::Key& key, uint32_t hash) {
        _finish_erase(_table.remove(key, hash));
    }

    void prune() {
        while (_lru._next != &_lru) {
            LRUHandle* e = _lru._next;
            assert(e->_refs == 1);
            bool erased = _finish_erase(_table.remove(e->_key, e->_hash));
            if (!erased) {
                // to avoid unused variable when compiled NDEBUG
                assert(erased);
            }
        }
    }

    size_t total_usage() const {
        return _usage;
    }

private:
    void _lru_remove(LRUHandle* e) {
        e->_next->_prev = e->_prev;
        e->_prev->_next = e->_next;
    }

    void _lru_append(LRUHandle* list, LRUHandle* e) {
        e->_next = list;
        e->_prev = list->_prev;
        e->_prev->_next = e;
        e->_next->_prev = e;
    }

    void _ref(LRUHandle* e) {
        if (e->_refs == 1 && e->_in_cache) {
            // If on lru_ list, move to in_use_ list.
            _lru_remove(e);
            _lru_append(&_in_use, e);
        }
        e->_refs++;
    }

    void _unref(LRUHandle* e) {
        assert(e->_refs > 0);
        e->_refs--;
        if (e->_refs == 0) {
            // Deallocate.
            assert(!e->_in_cache);
            e->_value.reset();
            delete e;
        } else if (e->_in_cache && e->_refs == 1) {
            // No longer in use; move to lru_ list.
            _lru_remove(e);
            _lru_append(&_lru, e);
        }
    }

    bool _finish_erase(LRUHandle* e) {
        if (e != nullptr) {
            assert(e->_in_cache);
            _lru_remove(e);
            e->_in_cache = false;
            _usage -= 1;
            _unref(e);
        }
        return e != nullptr;
    }

    size_t _capacity;
    mutable std::mutex _latch;
    size_t _usage;

    // lru 双向链表的空表头
    // lru.prev 指向最新的条目，lru.next 指向最老的条目
    // 此链表中所有条目都满足 refs==1 和 in_cache==true
    // 表示所有条目只被缓存引用，而没有客户端在使用
    LRUHandle _lru;      // 为双向链表，定义了每个哈希表的节点。lru双向链表中的节点是可以进行淘汰的。
    // in-use 双向链表的空表头
    // 保存所有仍然被客户端引用的条目
    // 由于在被客户端引用的同时还被缓存引用，
    // 肯定有 refs >= 2 和 in_cache==true.
    LRUHandle _in_use;   // 为双向链表，定义了每个哈希表的节点。in use双向链表中的节点表示正在使用，是不可以进行淘汰的
    HandleHashTable _table;  // 定义了一个哈希表，成员变量包括哈希表的桶个数，元素个数，桶的首地址
};

}