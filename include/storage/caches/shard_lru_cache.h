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
#include "lru_cache.h"

namespace LindormContest::storage {

// static const int kNumShardBits = 4;
// static const int kNumShards = 1 << kNumShardBits;
//
// class ShardedLRUCache : public Cache {
// private:
//     LRUCache _shard[kNumShards];
//     port::Mutex id_mutex_;
//     uint64_t last_id_;
//
//     static inline uint32_t HashSlice(const Slice& s) {
//         return Hash(s.data(), s.size(), 0);
//     }
//
//     static uint32_t Shard(uint32_t hash) {
//         return hash >> (32 - kNumShardBits);
//     }
//
// public:
//     explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
//         const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
//         for (int s = 0; s < kNumShards; s++) {
//             shard_[s].SetCapacity(per_shard);
//         }
//     }
//
//     ~ShardedLRUCache() override {}
//
//     Handle* Insert(const Slice& key, void* value, size_t charge,
//                    void (*deleter)(const Slice& key, void* value)) override {
//         const uint32_t hash = HashSlice(key);
//         return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
//     }
//
//     Handle* Lookup(const Slice& key) override {
//         const uint32_t hash = HashSlice(key);
//         return shard_[Shard(hash)].Lookup(key, hash);
//     }
//
//     void Release(Handle* handle) override {
//         LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
//         shard_[Shard(h->hash)].Release(handle);
//     }
//
//     void Erase(const Slice& key) override {
//         const uint32_t hash = HashSlice(key);
//         shard_[Shard(hash)].Erase(key, hash);
//     }
//
//     // 获取句柄中的值，类型为 void*（表示任意用户自定义类型）
//     // 要求：该句柄没有被释放
//     // 要求：该句柄必须由同一实例所返回
//     void* Value(Handle* handle) override {
//         return reinterpret_cast<LRUHandle*>(handle)->value;
//     }
//
//     // 返回一个自增的数值 id。当一个缓存实例由多个客户端共享时，
//     // 为了避免多个客户端的键冲突，每个客户端可能想获取一个独有
//     // 的 id，并将其作为键的前缀。类似于给每个客户端一个单独的命名空间。
//     uint64_t NewId() override {
//         MutexLock l(&id_mutex_);
//         return ++(last_id_);
//     }
//
//     void Prune() override {
//         for (int s = 0; s < kNumShards; s++) {
//             shard_[s].Prune();
//         }
//     }
//
//     size_t TotalCharge() const override {
//         size_t total = 0;
//         for (int s = 0; s < kNumShards; s++) {
//             total += shard_[s].TotalCharge();
//         }
//         return total;
//     }
// };

}