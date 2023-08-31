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
#include <mutex>

namespace LindormContest {
//    template<typename T>
//    class ConcurrentQueue {
//    public:
//        ConcurrentQueue() = default;
//
//        ConcurrentQueue(ConcurrentQueue &&other) noexcept = default;
//
//        ~ConcurrentQueue() = default;
//
//        bool empty() {
//            std::lock_guard<std::mutex> lock(_mutex);
//            return _queue.empty();
//        }
//
//        int size() {
//            std::lock_guard<std::mutex> lock(_mutex);
//            return _queue.size();
//        }
//
//        void enqueue(T &t) {
//            std::lock_guard<std::mutex> lock(_mutex);
//            _queue.emplace(t);
//        }
//
//        bool dequeue(T &t) {
//            std::lock_guard<std::mutex> lock(_mutex);
//            if (_queue.empty()) {
//                return false;
//            }
//            t = std::move(_queue.front());
//            _queue.pop();
//            return true;
//        }
//
//    private:
//        std::queue<T> _queue;
//        std::mutex _mutex;
//    };
 template<typename T>
    struct Node
    {
        Node(){};
        Node(const T &value) : data(value) { }
        ~Node() = default;
        T data;
        Node *next = nullptr;
    };
    template<typename T>
    class WithTwoLockQueue
    {
        std::mutex H_lock;
        std::mutex T_lock;
        Node<T> *head = nullptr;
        Node<T> *tail = nullptr;
        //int size{0};
    public:
        WithTwoLockQueue()
        {
            auto *node = new Node<T>();
            node->next = nullptr;
            head = tail = node;
        }
        void push(const T &value)
        {
            auto *node = new Node<T>(value);
            node->next = nullptr;
            {
                std::lock_guard<std::mutex> lock(T_lock);
                tail->next = node;
                tail = node;
                //size++;
            }
        }
        bool pop(T &value){
            std::lock_guard<std::mutex> lock(H_lock);
            Node<T>* temp = head;
            Node<T>* new_head = temp->next;
            if(new_head== nullptr) {return false;}
            value = new_head->data;
            head = new_head;
            free(temp);//free可以不用锁
            //size--;
            return true;
        }
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
            //_thread_pool_cv.notify_all();
            for (auto & _thread : _threads) {
                if (_thread.joinable()) {
                    _thread.join();
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
            _queue.push(wrapper_func);
            //_thread_pool_cv.notify_one();
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
//                    {
//                        std::unique_lock<std::mutex> lock(_thread_pool->_thread_pool_mutex);
                          dequeued = _thread_pool->_queue.pop(std::ref(func));
//                        if (_thread_pool->_queue.empty()) {
//                            _thread_pool->_thread_pool_cv.wait(lock);
//                        }
//                        if(!dequeued){
//                            _thread_pool->_thread_pool_cv.wait(lock);
//                        }
//                    }
                    if (dequeued) {
                        func();
                    }
                }
            }

        private:
            ThreadPool *_thread_pool;
        };
        bool _shutdown;
        WithTwoLockQueue<std::function<void()>> _queue;
        std::vector<std::thread> _threads;
        std::mutex _thread_pool_mutex;
        std::condition_variable _thread_pool_cv;
    };
}