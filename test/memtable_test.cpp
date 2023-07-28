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

#include "struct/Row.h"
#include "storage/table_schema.h"
#include "storage/memtable.h"

namespace LindormContest::test {

using namespace storage;
using namespace vectorized;

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

inline int32_t generate_random_int32() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int32_t> dis(0, std::numeric_limits<int32_t>::max());
    return dis(gen);
}

inline int64_t generate_random_timestamp() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, std::numeric_limits<int64_t>::max());
    return dis(gen);
}

inline double_t generate_random_float64() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> dis(0.0, 1.0);
    return dis(gen);
}

inline Row generate_row() {
    std::map<std::string, ColumnValue> columns;
    ColumnValue col2_val(generate_random_string(20));
    columns.insert({"col2", col2_val});
    ColumnValue col3_val(generate_random_float64());
    columns.insert({"col3", col3_val});
    ColumnValue col4_val(generate_random_int32());
    columns.insert({"col4", col4_val});
    Row row;
    row.vin = Vin(generate_random_string(17));
    row.timestamp = generate_random_timestamp();
    row.columns = std::move(columns);
    return row;
}

TEST(MemTableTest, BasicMemTableTest) {
    std::map<std::string, ColumnType> columnTypeMap;
    columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
    columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
    columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
    Schema schema;
    schema.columnTypeMap = std::move(columnTypeMap);
    std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
    std::unique_ptr<MemTable> mem_table = std::make_unique<MemTable>(table_schema.get());
    Block block = table_schema->create_block();
    MutableBlock mutable_block = MutableBlock::build_mutable_block(&block);

    const size_t BLOCK_SIZE = 10000;

    for (int i = 0; i < BLOCK_SIZE; ++i) {
        mutable_block.add_row(generate_row());
    }

    ASSERT_EQ(BLOCK_SIZE, mutable_block.rows());
    Block new_block = mutable_block.to_block();
    ASSERT_EQ(5, new_block.columns());
    ASSERT_EQ(BLOCK_SIZE, new_block.rows());
    mem_table->insert(std::move(new_block));
    ASSERT_EQ(BLOCK_SIZE, mem_table->rows());
    Block flush_block = mem_table->flush();
    ASSERT_EQ(BLOCK_SIZE, flush_block.rows());
}

TEST(MemTableTest, MultiMemTableTest) {
    std::map<std::string, ColumnType> columnTypeMap;
    columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
    columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
    columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
    Schema schema;
    schema.columnTypeMap = std::move(columnTypeMap);
    std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
    std::unique_ptr<MemTable> mem_table = std::make_unique<MemTable>(table_schema.get());
    Block block = table_schema->create_block();
    MutableBlock mutable_block = MutableBlock::build_mutable_block(&block);

    const size_t BLOCK_SIZE = 10000;

    for (int i = 0; i < BLOCK_SIZE; ++i) {
        Row row = generate_row();
        mutable_block.add_row(row);
        mutable_block.add_row(row);
    }

    ASSERT_EQ(BLOCK_SIZE * 2, mutable_block.rows());
    Block new_block = mutable_block.to_block();
    ASSERT_EQ(5, new_block.columns());
    ASSERT_EQ(BLOCK_SIZE * 2, new_block.rows());
    mem_table->insert(std::move(new_block));
    ASSERT_EQ(BLOCK_SIZE, mem_table->rows());
    Block flush_block = mem_table->flush();
    ASSERT_EQ(BLOCK_SIZE, flush_block.rows());
}

TEST(MemTableTest, ContentMemTableTest) {
    std::map<std::string, ColumnType> columnTypeMap;
    columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
    columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
    columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
    Schema schema;
    schema.columnTypeMap = std::move(columnTypeMap);
    std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
    std::unique_ptr<MemTable> mem_table = std::make_unique<MemTable>(table_schema.get());
    Block block1 = table_schema->create_block();
    Block block2 = table_schema->create_block();
    MutableBlock mutable_block = MutableBlock::build_mutable_block(&block1);
    MutableBlock target_mutable_block = MutableBlock::build_mutable_block(&block2);

    const size_t BLOCK_SIZE = 10000;

    std::vector<Row> rows;

    for (int i = 0; i < BLOCK_SIZE; ++i) {
        Row row = generate_row();
        mutable_block.add_row(row);
        rows.emplace_back(row);
    }

    std::sort(rows.begin(), rows.end(), [](const Row& lhs, const Row& rhs) -> bool {
        if (lhs.vin == rhs.vin) {
            return lhs.timestamp < rhs.timestamp;
        }
        return lhs.vin < rhs.vin;
    });

    for (const auto& row : rows) {
        target_mutable_block.add_row(row);
    }

    Block target_block = target_mutable_block.to_block();

    ASSERT_EQ(BLOCK_SIZE, mutable_block.rows());
    Block new_block = mutable_block.to_block();
    ASSERT_EQ(5, new_block.columns());
    ASSERT_EQ(BLOCK_SIZE, new_block.rows());
    mem_table->insert(std::move(new_block));
    ASSERT_EQ(BLOCK_SIZE, mem_table->rows());
    Block flush_block = mem_table->flush();

    ASSERT_EQ(target_block, flush_block);
}

}

