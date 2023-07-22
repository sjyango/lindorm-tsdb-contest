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

#include "vec/block.h"

namespace LindormContest::vectorized {

void Block::insert(size_t position, ColumnPtr column) {
    if (position > data.size()) {
        std::cerr << "Position out of bound in Block::insert(), max position = " << data.size() << std::endl;
    }

    for (auto& name_pos : index_by_name) {
        if (name_pos.second >= position) {
            ++name_pos.second;
        }
    }

    index_by_name.emplace(column->get_name(), position);
    data.emplace(data.begin() + position, column);
}

void Block::insert(ColumnPtr column) {
    index_by_name.emplace(column->get_name(), data.size());
    data.emplace_back(column);
}

void Block::erase(size_t position) {
    if (data.empty()) {
        std::cerr << "Block is empty" << std::endl;
    }

    if (position >= data.size()) {
        std::cerr << "Position out of bound in Block::erase(), max position = " << data.size() - 1 << std::endl;
    }

    erase_impl(position);
}

void Block::erase(const std::set<size_t>& positions) {
    for (auto it = positions.rbegin(); it != positions.rend(); ++it) {
        erase(*it);
    }
}

void Block::erase(const String& name) {
    auto it = index_by_name.find(name);
    if (it == index_by_name.end()) {
        std::cerr << "No such name in Block::erase(): " << name << std::endl;
    }

    erase_impl(it->second);
}

void Block::erase_impl(size_t position) {
    data.erase(data.begin() + position);

    for (auto it = index_by_name.begin(); it != index_by_name.end();) {
        if (it->second == position) {
            index_by_name.erase(it++);
        } else if (it->second > position) {
            --it->second;
        } else {
            ++it;
        }
    }
}

MutableColumns Block::mutate_columns() {
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        columns[i] = const_cast<MutableColumnPtr>(data[i]);
    }
    return columns;
}

}