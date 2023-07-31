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

#include <string>
#include <random>

#include <gtest/gtest.h>

#include "vec/columns/IColumn.h"
#include "vec/columns/ColumnFactory.h"

namespace LindormContest::test {

using namespace vectorized;

TEST(ColumnTest, ColumnInt32Test1) {
    std::shared_ptr<ColumnInt32> col1 = std::dynamic_pointer_cast<ColumnInt32>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_INTEGER, "col1"));
    std::shared_ptr<ColumnInt32> col2 = std::dynamic_pointer_cast<ColumnInt32>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_INTEGER, "col2"));

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
    std::vector<size_t> indices;

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

TEST(ColumnTest, ColumnInt32Test2) {
    const size_t N = 10000;
    std::shared_ptr<ColumnInt32> col1 = std::dynamic_pointer_cast<ColumnInt32>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_INTEGER, "col1"));
    std::shared_ptr<ColumnInt32> col2 = std::dynamic_pointer_cast<ColumnInt32>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_INTEGER, "col2"));
    std::vector<int32_t> nums;

    for (int i = 0; i < N; ++i) {
        nums.emplace_back(i);
        col1->push_number(i);
    }

    col2->insert_many_data(reinterpret_cast<const uint8_t*>(nums.data()), N);
    assert(col1->size() == col2->size());
    assert(N == col2->size());

    for (int i = 0; i <  N; ++i) {
        assert(col1->get(i) == col2->get(i));
        assert(i == col2->get(i));
    }
}

TEST(ColumnTest, ColumnInt64Test1) {
    std::shared_ptr<ColumnInt64> col1 = std::dynamic_pointer_cast<ColumnInt64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_TIMESTAMP, "col1"));
    std::shared_ptr<ColumnInt64> col2 = std::dynamic_pointer_cast<ColumnInt64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_TIMESTAMP, "col2"));

    ASSERT_EQ("col1", col1->get_name());
    ASSERT_EQ(ColumnType::COLUMN_TYPE_TIMESTAMP, col1->get_type());
    ASSERT_EQ("col2", col2->get_name());
    ASSERT_EQ(ColumnType::COLUMN_TYPE_TIMESTAMP, col2->get_type());

    for (int64_t i = 0; i < 10000; ++i) {
        col1->push_number(i);
    }

    ASSERT_EQ(10000, col1->size());

    for (int64_t i = 0; i < 10000; ++i) {
        ASSERT_EQ(i, (*col1)[i]);
    }

    col2->insert_from(*col1, 0);
    ASSERT_EQ(1, col2->size());
    ASSERT_EQ(0, (*col2)[0]);

    col2->insert_range_from(*col1, 1, 4999);
    ASSERT_EQ(5000, col2->size());
    std::vector<size_t> indices;

    for (int64_t i = 0; i < 5000; ++i) {
        indices.push_back(5000 + i);
        ASSERT_EQ(i, (*col2)[i]);
    }

    col2->insert_indices_from(*col1, indices.data(), indices.data() + indices.size());
    ASSERT_EQ(col1->size(), col2->size());

    for (int64_t i = 0; i < 10000; ++i) {
        ASSERT_EQ(i, (*col2)[i]);
        ASSERT_EQ(0, col1->compare_at(i, i, *col2));
    }
}

TEST(ColumnTest, ColumnInt64Test2) {
    const size_t N = 10000;
    std::shared_ptr<ColumnInt64> col1 = std::dynamic_pointer_cast<ColumnInt64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_TIMESTAMP, "col1"));
    std::shared_ptr<ColumnInt64> col2 = std::dynamic_pointer_cast<ColumnInt64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_TIMESTAMP, "col2"));
    std::vector<int64_t> nums;

    for (int i = 0; i < N; ++i) {
        nums.emplace_back(i);
        col1->push_number(i);
    }

    col2->insert_many_data(reinterpret_cast<const uint8_t*>(nums.data()), N);
    assert(col1->size() == col2->size());
    assert(N == col2->size());

    for (int i = 0; i <  N; ++i) {
        assert(col1->get(i) == col2->get(i));
        assert(i == col2->get(i));
    }
}

TEST(ColumnTest, ColumnFloat64Test1) {
    std::shared_ptr<ColumnFloat64> col1 = std::dynamic_pointer_cast<ColumnFloat64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, "col1"));
    std::shared_ptr<ColumnFloat64> col2 = std::dynamic_pointer_cast<ColumnFloat64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, "col2"));

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
    std::vector<size_t> indices;

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

TEST(ColumnTest, ColumnFloat64Test2) {
    const size_t N = 10000;
    std::shared_ptr<ColumnFloat64> col1 = std::dynamic_pointer_cast<ColumnFloat64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, "col1"));
    std::shared_ptr<ColumnFloat64> col2 = std::dynamic_pointer_cast<ColumnFloat64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, "col2"));
    std::vector<double_t> nums;

    for (int i = 0; i < N; ++i) {
        nums.emplace_back(i * 1.1);
        col1->push_number(i * 1.1);
    }

    col2->insert_many_data(reinterpret_cast<const uint8_t*>(nums.data()), N);
    assert(col1->size() == col2->size());
    assert(N == col2->size());

    for (int i = 0; i <  N; ++i) {
        assert(col1->get(i) == col2->get(i));
        assert(i * 1.1 == col2->get(i));
    }
}

inline std::string generate_random_string(int length) {
    const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, charset.size() - 1);

    std::string str(length, '\0');
    for (int i = 0; i < length; ++i) {
        str[i] = charset[dis(gen)];
    }

    return str;
}

inline int32_t generate_random_int(int32_t bound) {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int32_t> dis(0, std::numeric_limits<int32_t>::max());
    return dis(gen) % bound; // [0, bound)
}

TEST(ColumnTest, ColumnStringTest1) {
    std::shared_ptr<ColumnString> col1 = std::dynamic_pointer_cast<ColumnString>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_STRING, "col1"));
    std::shared_ptr<ColumnString> col2 = std::dynamic_pointer_cast<ColumnString>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_STRING, "col2"));

    ASSERT_EQ("col1", col1->get_name());
    ASSERT_EQ(ColumnType::COLUMN_TYPE_STRING, col1->get_type());
    ASSERT_EQ("col2", col2->get_name());
    ASSERT_EQ(ColumnType::COLUMN_TYPE_STRING, col2->get_type());

    for (int32_t i = 0; i < 10000; ++i) {
        col1->push_string(std::to_string(i));
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
    std::vector<size_t> indices;

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

TEST(ColumnTest, ColumnStringTest2) {
    const int N = 10000;
    std::shared_ptr<ColumnString> col1 = std::dynamic_pointer_cast<ColumnString>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_STRING, "col1"));
    std::shared_ptr<ColumnString> col2 = std::dynamic_pointer_cast<ColumnString>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_STRING, "col2"));
    std::string data;
    std::vector<uint32_t> offsets;

    for (int i = 0; i < N; ++i) {
        std::string s = generate_random_string(generate_random_int(100));
        data.append(s);
        offsets.push_back(data.size());
        col1->push_string(s);
    }

    col2->insert_binary_data(data.data(), offsets.data(), N);
    ASSERT_EQ(N, col1->size());
    ASSERT_EQ(N, col2->size());

    for (int32_t i = 0; i < N; ++i) {
        ASSERT_EQ(col1->get(i), col2->get(i));
    }

    col2->insert_binary_data(data.data(), offsets.data(), N);
    ASSERT_EQ(N * 2, col2->size());

    for (int32_t i = 0; i < N; ++i) {
        ASSERT_EQ(col1->get(i), col2->get(i + N));
    }
}

}

