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
#include <queue>
#include <functional>
#include <future>
#include <thread>
#include <utility>
#include <vector>

#include "Root.h"

namespace LindormContest {

    template<typename T>
    class ConcurrentQueue {
    public:
        ConcurrentQueue() = default;

        ConcurrentQueue(ConcurrentQueue &&other) noexcept = default;

        ~ConcurrentQueue() = default;

        bool empty() {
            std::unique_lock<std::mutex> lock(_mutex);
            return _queue.empty();
        }

        int size() {
            std::unique_lock<std::mutex> lock(_mutex);
            return _queue.size();
        }

        void enqueue(T &t) {
            std::unique_lock<std::mutex> lock(_mutex);
            _queue.emplace(t);
        }

        bool dequeue(T &t) {
            std::unique_lock<std::mutex> lock(_mutex);
            if (_queue.empty()) {
                return false;
            }
            t = std::move(_queue.front());
            _queue.pop();
            return true;
        }

    private:
        std::queue<T> _queue;
        std::mutex _mutex;
    };

    class ThreadPool {
    public:
        explicit ThreadPool(const int thread_nums)
                : _threads(std::vector<std::thread>(thread_nums)), _shutdown(false) {}

        ThreadPool(const ThreadPool &) = delete;

        ThreadPool(ThreadPool &&) = delete;

        ThreadPool &operator=(const ThreadPool &) = delete;

        ThreadPool &operator=(ThreadPool &&) = delete;

        void init() {
            for (auto & _thread : _threads) {
                _thread = std::thread(ThreadWorker(this));
            }
        }

        void shutdown() {
            _shutdown = true;
            _thread_pool_cv.notify_all();
            for (auto & thread : _threads) {
                if (thread.joinable()) {
                    thread.join();
                }
            }
        }

        template<typename F, typename... Args>
        auto submit(F &&f, Args &&...args) -> std::future<decltype(f(args...))> {
            auto func = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
            auto task_ptr = std::make_shared<std::packaged_task<decltype(f(args...))()>>(func);

            std::function<void()> wrapper_func = [task_ptr]() {
                (*task_ptr)();
            };

            _queue.enqueue(wrapper_func);
            _thread_pool_cv.notify_one();
            return task_ptr->get_future();
        }

    private:
        class ThreadWorker {
        public:
            explicit ThreadWorker(ThreadPool *pool) : _thread_pool(pool) {}

            void operator()() {
                std::function<void()> func;
                bool dequeued;

                while (!_thread_pool->_shutdown) {
                    {
                        std::unique_lock<std::mutex> lock(_thread_pool->_thread_pool_mutex);
                        if (_thread_pool->_queue.empty()) {
                            _thread_pool->_thread_pool_cv.wait(lock);
                        }
                        dequeued = _thread_pool->_queue.dequeue(func);
                    }
                    if (dequeued) {
                        func();
                    }
                }
            }

        private:
            ThreadPool *_thread_pool;
        };

        bool _shutdown;
        ConcurrentQueue<std::function<void()>> _queue;
        std::vector<std::thread> _threads;
        std::mutex _thread_pool_mutex;
        std::condition_variable _thread_pool_cv;
    };
}