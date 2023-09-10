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

#include <gtest/gtest.h>

#include "storage/tsm_writer.h"
#include "struct/Schema.h"

namespace LindormContest::test {
    TEST(TsmTest, BasicTsmTest) {
        SchemaSPtr schema = std::make_shared<Schema>();
        schema->columnTypeMap.insert({"col1", COLUMN_TYPE_STRING});
        schema->columnTypeMap.insert({"col2", COLUMN_TYPE_INTEGER});
        schema->columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
    }
}