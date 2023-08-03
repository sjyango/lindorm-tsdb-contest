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

#include <map>

#include <gtest/gtest.h>

#include "storage/table_schema.h"

namespace LindormContest::test {

using namespace storage;
using namespace vectorized;

TEST(SchemaTest, BasicSchemaTest) {
    std::map<std::string, ColumnType> columnTypeMap;
    columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
    columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
    columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
    Schema schema;
    schema.columnTypeMap = std::move(columnTypeMap);
    std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
    ASSERT_EQ(5, table_schema->num_columns());
    ASSERT_EQ(2, table_schema->num_key_columns());
    ASSERT_EQ(2, table_schema->num_short_key_columns());
    TableColumn col0 = {0, "vin", ColumnType::COLUMN_TYPE_STRING, true};
    TableColumn col1 = {1, "timestamp", ColumnType::COLUMN_TYPE_TIMESTAMP, true};
    TableColumn col2 = {2, "col2", ColumnType::COLUMN_TYPE_STRING, false};
    TableColumn col3 = {3, "col3", ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, false};
    TableColumn col4 = {4, "col4", ColumnType::COLUMN_TYPE_INTEGER, false};
    ASSERT_EQ(col0, table_schema->column(0));
    ASSERT_EQ(col1, table_schema->column(1));
    ASSERT_EQ(col2, table_schema->column(2));
    ASSERT_EQ(col3, table_schema->column(3));
    ASSERT_EQ(col4, table_schema->column(4));
    ASSERT_TRUE(table_schema->have_column("vin"));
    ASSERT_TRUE(table_schema->have_column("timestamp"));
    ASSERT_TRUE(table_schema->have_column("col2"));
    ASSERT_TRUE(table_schema->have_column("col3"));
    ASSERT_TRUE(table_schema->have_column("col4"));
    ASSERT_EQ(col0, table_schema->column_by_name("vin"));
    ASSERT_EQ(col1, table_schema->column_by_name("timestamp"));
    ASSERT_EQ(col2, table_schema->column_by_name("col2"));
    ASSERT_EQ(col3, table_schema->column_by_name("col3"));
    ASSERT_EQ(col4, table_schema->column_by_name("col4"));
    ASSERT_TRUE(table_schema->have_column(0));
    ASSERT_TRUE(table_schema->have_column(1));
    ASSERT_TRUE(table_schema->have_column(2));
    ASSERT_TRUE(table_schema->have_column(3));
    ASSERT_TRUE(table_schema->have_column(4));
    ASSERT_EQ(col0, table_schema->column_by_uid(0));
    ASSERT_EQ(col1, table_schema->column_by_uid(1));
    ASSERT_EQ(col2, table_schema->column_by_uid(2));
    ASSERT_EQ(col3, table_schema->column_by_uid(3));
    ASSERT_EQ(col4, table_schema->column_by_uid(4));
    Block block = table_schema->create_block();
    ASSERT_EQ(5, block.columns());
    ASSERT_NE(nullptr, block.get_by_position(0)._column);
    ASSERT_EQ(COLUMN_TYPE_STRING, block.get_by_position(0)._type);
    ASSERT_EQ("vin", block.get_by_position(0)._name);
    ASSERT_NE(nullptr, block.get_by_position(1)._column);
    ASSERT_EQ(COLUMN_TYPE_TIMESTAMP, block.get_by_position(1)._type);
    ASSERT_EQ("timestamp", block.get_by_position(1)._name);
    ASSERT_NE(nullptr, block.get_by_position(2)._column);
    ASSERT_EQ(COLUMN_TYPE_STRING, block.get_by_position(2)._type);
    ASSERT_EQ("col2", block.get_by_position(2)._name);
    ASSERT_NE(nullptr, block.get_by_position(3)._column);
    ASSERT_EQ(COLUMN_TYPE_DOUBLE_FLOAT, block.get_by_position(3)._type);
    ASSERT_EQ("col3", block.get_by_position(3)._name);
    ASSERT_NE(nullptr, block.get_by_position(4)._column);
    ASSERT_EQ(COLUMN_TYPE_INTEGER, block.get_by_position(4)._type);
    ASSERT_EQ("col4", block.get_by_position(4)._name);
    Block partial_block = table_schema->create_block({0, 2, 4});
    ASSERT_EQ(3, partial_block.columns());
    ASSERT_NE(nullptr, partial_block.get_by_position(0)._column);
    ASSERT_EQ(COLUMN_TYPE_STRING, partial_block.get_by_position(0)._type);
    ASSERT_EQ("vin", partial_block.get_by_position(0)._name);
    ASSERT_NE(nullptr, partial_block.get_by_position(1)._column);
    ASSERT_EQ(COLUMN_TYPE_STRING, partial_block.get_by_position(1)._type);
    ASSERT_EQ("col2", partial_block.get_by_position(1)._name);
    ASSERT_NE(nullptr, partial_block.get_by_position(2)._column);
    ASSERT_EQ(COLUMN_TYPE_INTEGER, partial_block.get_by_position(2)._type);
    ASSERT_EQ("col4", partial_block.get_by_position(2)._name);
}

}

