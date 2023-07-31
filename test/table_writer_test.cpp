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

#include "storage/segment_traits.h"
#include "storage/table_writer.h"

namespace LindormContest::test {

using namespace storage;

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

inline ordinal_t generate_random_number(size_t l, size_t r) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(l, r);
    return dis(gen);
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

inline WriteRequest generate_write_request(std::string table_name) {
    size_t num_rows = generate_random_number(10000, 30000);
    std::vector<Row> rows;
    for (size_t i = 0; i < num_rows; ++i) {
        rows.push_back(generate_row());
    }
    return WriteRequest {table_name, std::move(rows)};
}

TEST(TableWriterTest, BasicTableWriterTest) {
    static const std::string TABLE_NAME = "test";
    Schema schema;
    schema.columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
    schema.columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
    schema.columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
    TableSchemaSPtr table_schema = std::make_shared<TableSchema>(schema);
    std::unique_ptr<TableWriter> table_writer = std::make_unique<TableWriter>(TABLE_NAME, table_schema);
    WriteRequest wq = generate_write_request(TABLE_NAME);
    std::optional<SegmentSPtr> segment_data = table_writer->append(wq);
    ASSERT_TRUE(segment_data.has_value());
}

}

