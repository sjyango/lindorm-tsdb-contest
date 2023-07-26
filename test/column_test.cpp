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

#include "vec/columns/IColumn.h"
#include "vec/columns/ColumnFactory.h"

namespace LindormContest::test {

using namespace vectorized;

TEST(ColumnTest, ColumnInt32Test) {
    ColumnInt32* col1 = (ColumnInt32*)ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_INTEGER, "col1");
    ColumnInt32* col2 = (ColumnInt32*)ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_INTEGER, "col2");

    ASSERT_EQ("col1", col1->get_name());
    ASSERT_EQ(ColumnType::COLUMN_TYPE_INTEGER, col1->get_type());
    ASSERT_EQ("col2", col2->get_name());
    ASSERT_EQ(ColumnType::COLUMN_TYPE_INTEGER, col2->get_type());

    for (int32_t i = 0; i < 10000; ++i) {
        col1->push_number(i);
    }

    ASSERT_EQ(10000, col1->size());

    for (int32_t i = 0; i < 10000; ++i) {
        ASSERT_EQ(i, (*col1)[i]);
    }

    col2->insert_from(*col1, 0);
    ASSERT_EQ(1, col2->size());
    ASSERT_EQ(0, (*col2)[0]);

    col2->insert_range_from(*col1, 1, 4999);
    ASSERT_EQ(5000, col2->size());
    std::vector<int> indices;

    for (int32_t i = 0; i < 5000; ++i) {
        indices.push_back(5000 + i);
        ASSERT_EQ(i, (*col2)[i]);
    }

    col2->insert_indices_from(*col1, indices.data(), indices.data() + indices.size());
    ASSERT_EQ(col1->size(), col2->size());

    for (int32_t i = 0; i < 10000; ++i) {
        ASSERT_EQ(i, (*col2)[i]);
        ASSERT_EQ(0, col1->compare_at(i, i, *col2));
    }
}

TEST(ColumnTest, ColumnFloat64Test) {
    ColumnFloat64* col1 = (ColumnFloat64*)ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, "col1");
    ColumnFloat64* col2 = (ColumnFloat64*)ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, "col2");

    ASSERT_EQ("col1", col1->get_name());
    ASSERT_EQ(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, col1->get_type());
    ASSERT_EQ("col2", col2->get_name());
    ASSERT_EQ(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, col2->get_type());

    for (int32_t i = 0; i < 10000; ++i) {
        col1->push_number(i * 1.1);
    }

    ASSERT_EQ(10000, col1->size());

    for (int32_t i = 0; i < 10000; ++i) {
        ASSERT_EQ(i * 1.1, (*col1)[i]);
    }

    col2->insert_from(*col1, 0);
    ASSERT_EQ(1, col2->size());
    ASSERT_EQ(0.0, (*col2)[0]);

    col2->insert_range_from(*col1, 1, 4999);
    ASSERT_EQ(5000, col2->size());
    std::vector<int> indices;

    for (int32_t i = 0; i < 5000; ++i) {
        indices.push_back(5000 + i);
        ASSERT_EQ(i * 1.1, (*col2)[i]);
    }

    col2->insert_indices_from(*col1, indices.data(), indices.data() + indices.size());
    ASSERT_EQ(col1->size(), col2->size());

    for (int32_t i = 0; i < 10000; ++i) {
        ASSERT_EQ(i * 1.1, (*col2)[i]);
        ASSERT_EQ(0, col1->compare_at(i, i, *col2));
    }
}

TEST(ColumnTest, ColumnStringTest) {
    ColumnString* col1 = (ColumnString*)ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_STRING, "col1");
    ColumnString* col2 = (ColumnString*)ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_STRING, "col2");

    ASSERT_EQ("col1", col1->get_name());
    ASSERT_EQ(ColumnType::COLUMN_TYPE_STRING, col1->get_type());
    ASSERT_EQ("col2", col2->get_name());
    ASSERT_EQ(ColumnType::COLUMN_TYPE_STRING, col2->get_type());

    for (int32_t i = 0; i < 10000; ++i) {
        auto str_val = std::to_string(i);
        col1->push_string(str_val.c_str(), str_val.size());
    }

    ASSERT_EQ(10000, col1->size());

    for (int32_t i = 0; i < 10000; ++i) {
        ASSERT_EQ(std::to_string(i), (*col1)[i]);
    }

    col2->insert_from(*col1, 0);
    ASSERT_EQ(1, col2->size());
    ASSERT_EQ(std::to_string(0), (*col2)[0]);

    col2->insert_range_from(*col1, 1, 4999);
    ASSERT_EQ(5000, col2->size());
    std::vector<int> indices;

    for (int32_t i = 0; i < 5000; ++i) {
        indices.push_back(5000 + i);
        ASSERT_EQ(std::to_string(i), (*col2)[i]);
    }

    col2->insert_indices_from(*col1, indices.data(), indices.data() + indices.size());
    ASSERT_EQ(col1->size(), col2->size());

    for (int32_t i = 0; i < 10000; ++i) {
        ASSERT_EQ(std::to_string(i), (*col2)[i]);
        ASSERT_EQ(0, col1->compare_at(i, i, *col2));
    }
}

}

