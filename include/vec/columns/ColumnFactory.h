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
#include "vec/columns/ColumnString.h"
#include "vec/columns/ColumnNumber.h"

namespace LindormContest::vectorized {

class ColumnFactory {
public:
    static ColumnFactory& instance() {
        static ColumnFactory instance;
        return instance;
    }

    MutableColumnSPtr create_column(ColumnType type, String column_name) {
        switch (type) {
        case COLUMN_TYPE_INTEGER:
            return std::make_shared<ColumnInt32>(column_name);
        case COLUMN_TYPE_DOUBLE_FLOAT:
            return std::make_shared<ColumnFloat64>(column_name);
        case COLUMN_TYPE_STRING:
            return std::make_shared<ColumnString>(column_name);
        case COLUMN_TYPE_TIMESTAMP:
            return std::make_shared<ColumnInt64>(column_name);
        default:
            return nullptr;
        }
    }
};

} // namespace LindormContest::vectorized
