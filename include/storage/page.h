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
#include "common/slice.h"
#include "storage/segment_traits.h"

namespace LindormContest::storage {

struct Page {
    OwnedSlice _data;
    PageFooter _page_footer;
    std::shared_ptr<Page> _prev;
    std::shared_ptr<Page> _next;

    Page() = default;

    ~Page() = default;

    Page(OwnedSlice&& data, PageFooter page_footer,
         std::shared_ptr<Page> prev, std::shared_ptr<Page> next)
            : _data(std::move(data)),
              _page_footer(page_footer),
              _prev(prev), _next(next) {}

    Page(OwnedSlice&& data, PageFooter page_footer)
            : _data(std::move(data)),
              _page_footer(page_footer),
              _prev(nullptr), _next(nullptr) {}
};

class PageLinkedList {
public:
    PageLinkedList() : _head(nullptr), _tail(nullptr), _size(0) {}

    size_t size() const { return _size; }

    bool is_empty() const { return _size == 0; }

    void push_page_front(Page&& page) {
        std::shared_ptr<Page> new_page = std::make_shared<Page>(std::move(page._data), page._page_footer, nullptr, _head);
        if (_head == nullptr) {
            _tail = new_page;
        } else {
            _head->_prev = new_page;
        }
        _head = new_page;
        ++_size;
    }

    void push_page_back(Page&& page) {
        std::shared_ptr<Page> new_page = std::make_shared<Page>(std::move(page._data), page._page_footer, _tail, nullptr);
        if (_tail == nullptr) {
            _head = new_page;
        } else {
            _tail->_next = new_page;
        }
        _tail = new_page;
        ++_size;
    }

    void pop_page_front() {
        if (_head == nullptr) {
            return;
        }
        std::shared_ptr<Page> temp = _head;
        _head = temp->_next;
        if (_head == nullptr) {
            _tail = nullptr;
        } else {
            _head->_prev = nullptr;
        }
        --_size;
    }

    void pop_page_back() {
        if (_tail == nullptr) {
            return;
        }
        std::shared_ptr<Page> temp = _tail;
        _tail = temp->_prev;
        if (_tail == nullptr) {
            _head = nullptr;
        } else {
            _tail->_next = nullptr;
        }
        --_size;
    }

private:
    std::shared_ptr<Page> _head;
    std::shared_ptr<Page> _tail;
    size_t _size;
};

}