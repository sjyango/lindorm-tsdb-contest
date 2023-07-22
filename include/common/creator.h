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
#include <memory>

namespace LindormContest {

#define ENABLE_FACTORY_CREATOR(TypeName)                                             \
private:                                                                             \
    void* operator new(std::size_t size) {                                           \
        return ::operator new(size);                                                 \
    }                                                                                \
    void* operator new[](std::size_t size) {                                         \
        return ::operator new[](size);                                               \
    }                                                                                \
                                                                                     \
public:                                                                              \
    void* operator new(std::size_t count, void* ptr) {                               \
        return ::operator new(count, ptr);                                           \
    }                                                                                \
    void operator delete(void* ptr) noexcept {                                       \
        ::operator delete(ptr);                                                      \
    }                                                                                \
    void operator delete[](void* ptr) noexcept {                                     \
        ::operator delete[](ptr);                                                    \
    }                                                                                \
    void operator delete(void* ptr, void* place) noexcept {                          \
        ::operator delete(ptr, place);                                               \
    }                                                                                \
    template <typename... Args>                                                      \
    static std::shared_ptr<TypeName> create_shared(Args&&... args) {                 \
        return std::make_shared<TypeName>(std::forward<Args>(args)...);              \
    }                                                                                \
    template <typename... Args>                                                      \
    static std::unique_ptr<TypeName> create_unique(Args&&... args) {                 \
        return std::make_unique<TypeName>(std::forward<Args>(args)...);              \
    }


}
