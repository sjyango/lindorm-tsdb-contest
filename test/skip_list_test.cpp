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

#include <gtest/gtest.h>

#include "common/arena.h"
#include "storage/skiplist.h"

namespace LindormContest::test {

using namespace storage;

using Key = uint64_t;

struct Comparator {
    int operator()(const Key& a, const Key& b) const {
        if (a < b) {
            return -1;
        } else if (a > b) {
            return 1;
        } else {
            return 0;
        }
    }
};

TEST(SkipListTest, EmptyTest) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(&cmp, &arena);
    ASSERT_TRUE(!list.contains(10));

    SkipList<Key, Comparator>::Iterator iter(&list);
    ASSERT_TRUE(!iter.valid());
    iter.seek_to_first();
    ASSERT_TRUE(!iter.valid());
    iter.seek(100);
    ASSERT_TRUE(!iter.valid());
    iter.seek_to_last();
    ASSERT_TRUE(!iter.valid());
}

TEST(SkipTest, InsertAndLookupTest) {
    const int N = 2000;
    const int R = 5000;
    Random rnd(1000);
    std::set<Key> keys;
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(&cmp, &arena);

    for (int i = 0; i < N; i++) {
        Key key = rnd.Next() % R;
        if (keys.insert(key).second) {
            list.insert(key);
        }
    }
    
    for (int i = 0; i < R; i++) {
        if (list.contains(i)) {
            ASSERT_EQ(keys.count(i), 1);
        } else {
            ASSERT_EQ(keys.count(i), 0);
        }
    }

    // Simple iterator tests
    {
        SkipList<Key, Comparator>::Iterator iter(&list);
        ASSERT_TRUE(!iter.valid());

        iter.seek(0);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(*(keys.begin()), iter.key());

        iter.seek_to_first();
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(*(keys.begin()), iter.key());

        iter.seek_to_last();
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(*(keys.rbegin()), iter.key());
    }

    // Forward iteration test
    for (int i = 0; i < R; i++) {
        SkipList<Key, Comparator>::Iterator iter(&list);
        iter.seek(i);

        // Compare against model iterator
        std::set<Key>::iterator model_iter = keys.lower_bound(i);
        for (int j = 0; j < 3; j++) {
            if (model_iter == keys.end()) {
                ASSERT_TRUE(!iter.valid());
                break;
            } else {
                ASSERT_TRUE(iter.valid());
                ASSERT_EQ(*model_iter, iter.key());
                ++model_iter;
                iter.next();
            }
        }
    }

    // Backward iteration test
    {
        SkipList<Key, Comparator>::Iterator iter(&list);
        iter.seek_to_last();

        // Compare against model iterator
        for (std::set<Key>::reverse_iterator model_iter = keys.rbegin();
             model_iter != keys.rend(); ++model_iter) {
            ASSERT_TRUE(iter.valid());
            ASSERT_EQ(*model_iter, iter.key());
            iter.prev();
        }
        ASSERT_TRUE(!iter.valid());
    }
}

TEST(SkipTest, DuplicateInsertionTest) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(&cmp, &arena);
    SkipList<Key, Comparator>::Hint hint;

    list.insert(0);
    ASSERT_TRUE(list.find(0, &hint));
    hint.curr->key = 1; // 0 -> 1
    ASSERT_FALSE(list.find(0, &hint));
    ASSERT_TRUE(list.find(1, &hint));
    ASSERT_FALSE(list.find(2, &hint));
    list.insert_with_hint(2, false, &hint);
    ASSERT_TRUE(list.find(2, &hint));
}

}

