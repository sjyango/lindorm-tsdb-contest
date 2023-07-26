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

#include "IColumn.h"
#include "Root.h"
#include "ColumnFactory.h"

namespace LindormContest::vectorized {

class ColumnWithTypeAndName {
public:
    ColumnSPtr _column;
    ColumnType _type;
    String _name;

    ColumnWithTypeAndName() = default;

    ColumnWithTypeAndName(ColumnSPtr column, ColumnType type, const String& name)
            : _column(column), _type(type), _name(name) {}

    ColumnWithTypeAndName(ColumnType type, const String& name)
            : _column(ColumnFactory::instance().create_column(type, name)), _type(type), _name(name) {}

    ColumnWithTypeAndName clone_empty() const {
        ColumnWithTypeAndName res;
        res._type = _type;
        res._name = _name;
        if (_column) {
            res._column = _column->clone_empty();
        } else {
            res._column = nullptr;
        }
        return res;
    }
};

using ColumnsWithTypeAndName = std::vector<ColumnWithTypeAndName>;

} // namespace LindormContest::vectorized
