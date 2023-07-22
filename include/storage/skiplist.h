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

#include <atomic>

#include "common/random.h"
#include "common/arena.h"

namespace LindormContest::storage {

template <typename Key, class Comparator>
class SkipList {
    static const size_t MAXHEIGHT = 12;

private:
    struct Node {
        explicit Node(const Key& k) : key(k) {}

        const Key key;

        Node* next(int n) {
            assert(n >= 0);
            return _next[n].load(std::memory_order_acquire);
        }

        void set_next(int n, Node* x) {
            assert(n >= 0);
            _next[n].store(x, std::memory_order_release);
        }

        Node* no_barrier_next(int n) {
            assert(n >= 0);
            return _next[n].load(std::memory_order_relaxed);
        }

        void no_barrier_set_next(int n, Node* x) {
            assert(n >= 0);
            _next[n].store(x, std::memory_order_relaxed);
        }

    private:
        std::atomic<Node*> _next[1];
    };

public:
    struct Hint {
        Node* curr;
        Node* prev[MAXHEIGHT];
    };

    SkipList(Comparator* cmp, Arena* arena)
            : _compare(cmp),
              _arena(arena),
              _head(new_node(0, MAXHEIGHT)),
              _max_height(1),
              _rnd(0xdeadbeef) {
        for (int i = 0; i < MAXHEIGHT; i++) {
            _head->set_next(i, nullptr);
        }
    }

    // No copying allowed
    SkipList(const SkipList&) = delete;
    void operator=(const SkipList&) = delete;

    void insert(const Key& key, bool* overwritten) {
        Node* prev[MAXHEIGHT];
        Node* x = find_greater_or_equal(key, prev);

        *overwritten = false;
        // Our data structure does not allow duplicate insertion
        int height = random_height();
        if (height > get_max_height()) {
            for (int i = get_max_height(); i < height; i++) {
                prev[i] = _head;
            }
            _max_height.store(height, std::memory_order_relaxed);
        }

        x = new_node(key, height);
        
        for (int i = 0; i < height; i++) {
            x->no_barrier_set_next(i, prev[i]->no_barrier_next(i));
            prev[i]->set_next(i, x);
        }
    }
    
    // Use hint to insert a key. the hint is from previous Find()
    void insert_with_hint(const Key& key, bool is_exist, Hint* hint) {
        Node* x = hint->curr;
        assert(!is_exist || x);

        Node** prev = hint->prev;
        // Our data structure does not allow duplicate insertion
        int height = random_height();
        if (height > get_max_height()) {
            for (int i = get_max_height(); i < height; i++) {
                prev[i] = _head;
            }
            _max_height.store(height, std::memory_order_relaxed);
        }

        x = new_node(key, height);
        for (int i = 0; i < height; i++) {
            x->no_barrier_set_next(i, prev[i]->no_barrier_next(i));
            prev[i]->set_next(i, x);
        }
    }

    // Returns true if an entry that compares equal to key is in the list.
    bool contains(const Key& key) const {
        Node* x = find_greater_or_equal(key, nullptr);
        if (x != nullptr && equal(key, x->key)) {
            return true;
        } else {
            return false;
        }
    }

    // Like Contains(), but it will return the position info as a hint. We can use this
    // position info to insert directly using InsertWithHint().
    bool find(const Key& key, Hint* hint) const {
        Node* x = find_greater_or_equal(key, hint->prev);
        hint->curr = x;
        if (x != nullptr && equal(key, x->key)) {
            return true;
        } else {
            return false;
        }
    }

    class Iterator {
    public:
        explicit Iterator(const SkipList* list) {
            _list = list;
            _node = nullptr;
        }

        bool valid() const {
            return _node != nullptr;
        }

        const Key& key() const {
            assert(valid());
            return _node->key;
        }

        void next() {
            assert(valid());
            _node = _node->next(0);
        }

        void prev() {
            assert(valid());
            _node = _list->find_less_than(_node->key);
            if (_node == _list->_head) {
                _node = nullptr;
            }
        }

        void seek(const Key& target) {
            _node = _list->find_greater_or_equal(target, nullptr);
        }

        void seek_to_first() {
            _node = _list->_head->next(0);
        }

        void seek_to_last() {
            _node = _list->find_last();
            if (_node == _list->_head) {
                _node = nullptr;
            }
        }

    private:
        const SkipList* _list;
        Node* _node;
    };

private:
    int get_max_height() const { 
        return _max_height.load(std::memory_order_relaxed); 
    }
    
    Node* new_node(const Key& key, int height) {
        char* mem = static_cast<char*>(_arena->alloc(sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1)));
        return new (mem) Node(key);
    }
    
    int random_height() {
        static const unsigned int kBranching = 4;
        int height = 1;

        while (height < MAXHEIGHT && ((_rnd.Next() % kBranching) == 0)) {
            height++;
        }

        assert(height > 0);
        assert(height <= MAXHEIGHT);
        return height;
    }
    
    bool equal(const Key& a, const Key& b) const { 
        return ((*_compare)(a, b) == 0); 
    }

    bool key_is_after_node(const Key& key, Node* n) const {
        return (n != nullptr) && ((*_compare)(n->key, key) < 0);
    }

    Node* find_greater_or_equal(const Key& key, Node** prev) const {
        Node* x = _head;
        int level = get_max_height() - 1;
        while (true) {
            Node* next = x->next(level);
            if (key_is_after_node(key, next)) {
                x = next;
            } else {
                if (prev != nullptr) {
                    prev[level] = x;
                }
                if (level == 0) {
                    return next;
                } else {
                    level--;
                }
            }
        }
    }

    Node* find_less_than(const Key& key) const {
        Node* x = _head;
        int level = get_max_height() - 1;
        while (true) {
            assert(x == _head || (*_compare)(x->key, key) < 0);
            Node* next = x->next(level);
            if (next == nullptr || (*_compare)(next->key, key) >= 0) {
                if (level == 0) {
                    return x;
                } else {
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    Node* find_last() const {
        Node* x = _head;
        int level = get_max_height() - 1;
        while (true) {
            Node* next = x->next(level);
            if (next == nullptr) {
                if (level == 0) {
                    return x;
                } else {
                    level--;
                }
            } else {
                x = next;
            }
        }
    }
    
    Comparator* const _compare;
    Arena* const _arena;
    Node* const _head;
    std::atomic<int> _max_height;
    Random _rnd;
};

}