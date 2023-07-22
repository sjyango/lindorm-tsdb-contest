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

#include <unordered_map>

#include "Root.h"
#include "common/creator.h"
#include "vec/IColumn.h"

namespace LindormContest::vectorized {

class Block {
    ENABLE_FACTORY_CREATOR(Block);

    Block() = default;

    Block(std::initializer_list<ColumnPtr> il) : data {il} {
        initialize_index_by_name();
    }

    Block(const Columns& columns) : data {columns} {
        initialize_index_by_name();
    }

    void initialize_index_by_name() {
        for (size_t i = 0, size = data.size(); i < size; ++i) {
            index_by_name[data[i]->get_name()] = i;
        }
    }

    void reserve(size_t count) {
        index_by_name.reserve(count);
        data.reserve(count);
    }

    size_t columns() const {
        return data.size();
    }

    const Columns& get_columns() const {
        return data;
    }

    void clear() {
        MutableColumns columns = mutate_columns();
        for (auto& column : columns) {
            column->clear();
        }
        data.clear();
        index_by_name.clear();
    }

    void insert(size_t position, ColumnPtr column);

    void insert(ColumnPtr column);

    void erase(size_t position);

    void erase(const std::set<size_t>& positions);

    void erase(const String& name);

    MutableColumns mutate_columns();

private:
    void erase_impl(size_t position);

    using Container = Columns;
    using IndexByName = std::unordered_map<String, size_t>;

    Container data;
    IndexByName index_by_name;
};

}