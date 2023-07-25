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

#include "IDataType.h"
#include "struct/ColumnValue.h"

namespace LindormContest::vectorized {

class DataTypeInt32 : public IDataType {
public:
    ColumnType get_type() const override {
        return COLUMN_TYPE_INTEGER;
    }

    ColumnPtr create_column(String column_name) const {
        const IColumn* p = new ColumnString(column_name);
        return new ColumnNumber<Int32>();
    }
};

} // namespace LindormContest::vectorized
