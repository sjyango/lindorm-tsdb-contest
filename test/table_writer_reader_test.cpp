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

#include <random>

#include <gtest/gtest.h>

#include "TSDBEngineImpl.h"

namespace LindormContest::test {

using namespace storage;
using namespace vectorized;

static std::string generate_random_string(int length) {
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

static int32_t generate_random_int32() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int32_t> dis(0, std::numeric_limits<int32_t>::max());
    return dis(gen);
}

static int64_t generate_random_timestamp() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, std::numeric_limits<int64_t>::max());
    return dis(gen);
}

static double_t generate_random_float64() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> dis(0.0, 1.0);
    return dis(gen);
}

static Row generate_row() {
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

TEST(TableWriterReaderTest, BasicTableWriterReaderTest) {
    const size_t N = 10000;
    size_t MEM_TABLE_FLUSH_THRESHOLD = N / 10;
    std::atomic<size_t> next_segment_id = 0;

    // ######################################## TableWriter ########################################

    io::Path table_path = std::filesystem::current_path() / io::Path("test_data");
    io::FileSystemSPtr fs = io::FileSystem::create(table_path);
    std::map<std::string, ColumnType> columnTypeMap;
    columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
    columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
    columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
    Schema schema;
    schema.columnTypeMap = std::move(columnTypeMap);
    TableSchemaSPtr table_schema = std::make_shared<TableSchema>(schema);
    std::unique_ptr<TableWriter> table_writer = std::make_unique<TableWriter>(fs, table_schema, &next_segment_id, MEM_TABLE_FLUSH_THRESHOLD);
    std::vector<Row> src_rows;

    for (size_t i = 0; i < 10; ++i) {
        std::vector<Row> batch_rows;
        for (size_t j = 0; j < (N / 10); ++j) {
            batch_rows.emplace_back(generate_row());
        }
        table_writer->append(batch_rows);
        src_rows.insert(src_rows.end(), batch_rows.begin(), batch_rows.end());
    }

    table_writer->close();

    // ######################################## TableWriter ########################################

    // ######################################## TableReader ########################################

    std::unique_ptr<TableReader> table_reader = std::make_unique<TableReader>(fs, table_schema);
    PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table_schema);
    table_reader->init(partial_schema);
    std::vector<Row> dst_rows;
    size_t num_to_read = N / 10;
    table_reader->scan_all(&num_to_read, dst_rows);
    ASSERT_EQ(num_to_read, N / 10);

    // ######################################## TableReader ########################################

    std::sort(src_rows.begin(), src_rows.end(), [] (const Row& lhs, const Row& rhs) {
        if (lhs.vin != rhs.vin) {
            return lhs.vin < rhs.vin;
        }
        return lhs.timestamp < rhs.timestamp;
    });

    std::sort(dst_rows.begin(), dst_rows.end(), [] (const Row& lhs, const Row& rhs) {
        if (lhs.vin != rhs.vin) {
            return lhs.vin < rhs.vin;
        }
        return lhs.timestamp < rhs.timestamp;
    });

    ASSERT_EQ(src_rows, dst_rows);
}

TEST(TableWriterReaderTest, HandleLatestQueryTest) {
    const size_t N = 100000;
    size_t MEM_TABLE_FLUSH_THRESHOLD = N / 10;
    std::atomic<size_t> next_segment_id = 0;

    // ######################################## TableWriter ########################################

    io::Path table_path = std::filesystem::current_path() / io::Path("test_data");
    io::FileSystemSPtr fs = io::FileSystem::create(table_path);
    std::map<std::string, ColumnType> columnTypeMap;
    columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
    columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
    columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
    Schema schema;
    schema.columnTypeMap = std::move(columnTypeMap);
    TableSchemaSPtr table_schema = std::make_shared<TableSchema>(schema);
    std::unique_ptr<TableWriter> table_writer = std::make_unique<TableWriter>(fs, table_schema, &next_segment_id, MEM_TABLE_FLUSH_THRESHOLD);
    std::vector<Row> src_rows;

    for (size_t i = 0; i < 10; ++i) {
        std::vector<Row> batch_rows;
        for (size_t j = 0; j < (N / 10); ++j) {
            batch_rows.emplace_back(generate_row());
        }
        table_writer->append(batch_rows);
        src_rows.insert(src_rows.end(), batch_rows.begin(), batch_rows.end());
    }

    table_writer->close();

    // ######################################## TableWriter ########################################

    // ######################################## TableReader ########################################

    std::unique_ptr<TableReader> table_reader = std::make_unique<TableReader>(fs, table_schema);
    PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table_schema);
    table_reader->init(partial_schema);
    std::vector<Row> results;
    size_t rand_index = generate_random_int32() % src_rows.size();
    Vin rand_vin = src_rows[rand_index].vin;
    table_reader->handle_latest_query({rand_vin}, results);
    ASSERT_EQ(1, results.size());
    ASSERT_EQ(src_rows[rand_index], results[0]);

    // ######################################## TableReader ########################################
}

TEST(TableWriterReaderTest, HandleTimeRangeQueryTest) {
    const size_t N = 100000;
    size_t MEM_TABLE_FLUSH_THRESHOLD = N / 10;
    std::atomic<size_t> next_segment_id = 0;

    // ######################################## TableWriter ########################################

    io::Path table_path = std::filesystem::current_path() / io::Path("test_data");
    io::FileSystemSPtr fs = io::FileSystem::create(table_path);
    std::map<std::string, ColumnType> columnTypeMap;
    columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
    columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
    columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
    Schema schema;
    schema.columnTypeMap = std::move(columnTypeMap);
    TableSchemaSPtr table_schema = std::make_shared<TableSchema>(schema);
    std::unique_ptr<TableWriter> table_writer = std::make_unique<TableWriter>(fs, table_schema, &next_segment_id, MEM_TABLE_FLUSH_THRESHOLD);
    std::vector<Row> src_rows;

    for (size_t i = 0; i < 10; ++i) {
        std::vector<Row> batch_rows;
        for (size_t j = 0; j < (N / 10); ++j) {
            batch_rows.emplace_back(generate_row());
        }
        table_writer->append(batch_rows);
        src_rows.insert(src_rows.end(), batch_rows.begin(), batch_rows.end());
    }

    table_writer->close();

    // ######################################## TableWriter ########################################

    // ######################################## TableReader ########################################

    std::unique_ptr<TableReader> table_reader = std::make_unique<TableReader>(fs, table_schema);
    PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table_schema);
    table_reader->init(partial_schema);
    std::vector<Row> results;
    size_t rand_ordinal = generate_random_int32() % src_rows.size();
    size_t rand_timestamp = generate_random_timestamp();
    Vin rand_vin = src_rows[rand_ordinal].vin;
    std::vector<Row> query_results;
    std::vector<Row> ground_truths;
    table_reader->handle_time_range_query(rand_vin, 0, rand_timestamp, query_results);

    for (const auto& row : src_rows) {
        if (row.vin == rand_vin && row.timestamp >= 0 && row.timestamp < rand_timestamp) {
            std::string s(row.vin.vin, 17);
            ground_truths.emplace_back(row);
        }
    }

    ASSERT_EQ(query_results, ground_truths);

    // ######################################## TableReader ########################################
}

}

