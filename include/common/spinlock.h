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

namespace LindormContest {

    class SpinLock {
    public:
        SpinLock() : _locked(false) {}

        // Acquires the lock, spins until the lock becomes available
        void lock() {
            for (int spin_count = 0; !try_lock(); ++spin_count) {
                if (spin_count < NUM_SPIN_CYCLES) {
#if (defined(__i386) || defined(__x86_64__))
                    asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
                    asm volatile("yield\n" ::: "memory");
#endif
                } else {
                    sched_yield();
                    spin_count = 0;
                }
            }
        }

        void unlock() { _locked.clear(std::memory_order_release); }

        // Tries to acquire the lock
        bool try_lock() { return !_locked.test_and_set(std::memory_order_acquire); }

    private:
        static const int NUM_SPIN_CYCLES = 70;
        std::atomic_flag _locked;
    };

}