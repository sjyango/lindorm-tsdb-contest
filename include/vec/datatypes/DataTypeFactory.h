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
#include "IDataType.h"
#include "DataTypeString.h"
#include "DataTypeFloat64.h"
#include "DataTypeInt32.h"

namespace LindormContest::vectorized {

class DataTypeFactory {
public:
    static DataTypeFactory& instance() {
        static DataTypeFactory instance;
        return instance;
    }

    DataTypePtr create_data_type(ColumnType type) {
        switch (type) {
        case COLUMN_TYPE_STRING:
            return std::make_shared<DataTypeString>();
        case COLUMN_TYPE_INTEGER:
            return std::make_shared<DataTypeInt32>();
        case COLUMN_TYPE_DOUBLE_FLOAT:
            return std::make_shared<DataTypeFloat64>();
        case COLUMN_TYPE_UNINITIALIZED:
            return nullptr;
        }
    }
};

} // namespace LindormContest::vectorized
