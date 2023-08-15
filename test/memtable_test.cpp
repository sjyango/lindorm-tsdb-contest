// /*
// * Copyright Alibaba Group Holding Ltd.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// #include <string>
// #include <random>
// #include <filesystem>
// #include <queue>
// #include <unordered_map>
// #include <optional>
//
// #include <gtest/gtest.h>
//
// #include "struct/Row.h"
// #include "storage/table_schema.h"
// #include "storage/memtable.h"
// #include "io/file_writer.h"
// #include "io/file_system.h"
// #include "utils.h"
//
// namespace LindormContest::test {
//
// using namespace storage;
// using namespace vectorized;
//
// static Vin generate_vin(std::string s) {
//     Vin vin;
//     std::strncpy(vin.vin, s.c_str(), 17);
//     return vin;
// }
//
// inline std::string generate_random_string(int length) {
//     const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
//
//     std::random_device rd;
//     std::mt19937 gen(rd());
//     std::uniform_int_distribution<> dis(0, charset.size() - 1);
//
//     std::string str(length, '\0');
//     for (int i = 0; i < length; ++i) {
//         str[i] = charset[dis(gen)];
//     }
//
//     return str;
// }
//
// inline int32_t generate_random_int32() {
//     std::random_device rd;
//     std::mt19937_64 gen(rd());
//     std::uniform_int_distribution<int32_t> dis(0, std::numeric_limits<int32_t>::max());
//     return dis(gen);
// }
//
// inline int64_t generate_random_timestamp() {
//     std::random_device rd;
//     std::mt19937_64 gen(rd());
//     std::uniform_int_distribution<int64_t> dis(0, std::numeric_limits<int64_t>::max());
//     return dis(gen);
// }
//
// inline double_t generate_random_float64() {
//     std::random_device rd;
//     std::mt19937 gen(rd());
//     std::uniform_real_distribution<double> dis(0.0, 1.0);
//     return dis(gen);
// }
//
// inline io::FileWriterPtr generate_file_writer() {
//     size_t segment_id = 0;
//     io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
//     io::Path segment_path = root_path / io::Path("segment_" + std::to_string(segment_id) + ".dat");
//     io::FileSystemSPtr fs = io::FileSystem::create(root_path);
//     if (fs->exists(segment_path)) {
//         fs->delete_file(segment_path);
//     }
//     assert(!fs->exists(segment_path));
//     return fs->create_file(segment_path);
// }
//
// inline Row generate_row() {
//     std::map<std::string, ColumnValue> columns;
//     ColumnValue col2_val(generate_random_string(20));
//     columns.insert({"col2", col2_val});
//     ColumnValue col3_val(generate_random_float64());
//     columns.insert({"col3", col3_val});
//     ColumnValue col4_val(generate_random_int32());
//     columns.insert({"col4", col4_val});
//     Row row;
//     row.vin = generate_vin(generate_random_string(17));
//     row.timestamp = generate_random_timestamp();
//     row.columns = std::move(columns);
//     return row;
// }
//
// static size_t generate_dataset(std::vector<Row>& rows, std::unordered_map<std::string , std::priority_queue<int64_t>>& maps) {
//     const size_t BLOCK_SIZE = 1000;
//     const size_t BLOCK_RANGE = 100;
//
//     std::map<std::string, ColumnValue> columns;
//     ColumnValue col2_val(generate_random_string(20));
//     columns.insert({"col2", col2_val});
//     ColumnValue col3_val(generate_random_float64());
//     columns.insert({"col3", col3_val});
//     ColumnValue col4_val(generate_random_int32());
//     columns.insert({"col4", col4_val});
//
//     for (size_t i = 0; i < BLOCK_SIZE; ++i) {
//         std::string rand_vin = generate_random_string(17);
//         std::priority_queue<int64_t> pq;
//         maps.emplace(rand_vin, std::move(pq));
//
//         for (size_t j = 0; j < BLOCK_RANGE; ++j) {
//             Row row;
//             row.vin = generate_vin(rand_vin);
//             row.timestamp = generate_random_timestamp();
//             row.columns = columns;
//             maps[rand_vin].push(row.timestamp);
//             rows.emplace_back(std::move(row));
//         }
//     }
//
//     return BLOCK_SIZE * BLOCK_RANGE;
// }
//
// static std::optional<int64_t> handle_latest_query(Vin input_vin, std::unordered_map<std::string, std::priority_queue<int64_t>>& maps) {
//     std::string target_vin = {input_vin.vin, 17};
//
//     if (maps.find(target_vin) == maps.end()) {
//         return std::nullopt;
//     } else {
//         return {maps[target_vin].top()};
//     }
// }
//
// static std::optional<std::vector<int64_t>> handle_time_range_query(Vin input_vin, int64_t lower_time_bound, int64_t upper_time_bound, std::unordered_map<std::string, std::priority_queue<int64_t>>& maps) {
//     std::string target_vin = {input_vin.vin, 17};
//     if (maps.find(target_vin) == maps.end()) {
//         return std::nullopt;
//     }
//
//     auto& pq = maps[target_vin];
//     std::vector<int64_t> results;
//     while (!pq.empty()) {
//         int64_t time = pq.top();
//         pq.pop();
//         if (time >= lower_time_bound && time < upper_time_bound) {
//             results.push_back(time);
//         }
//     }
//     if (results.empty()) {
//         return std::nullopt;
//     }
//     return {results};
// }
//
// TEST(MemTableTest, BasicMemTableTest) {
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::unique_ptr<MemTable> mem_table = std::make_unique<MemTable>(file_writer.get(), table_schema, 0);
//     Block block = table_schema->create_block();
//     MutableBlock mutable_block = MutableBlock::build_mutable_block(&block);
//
//     const size_t BLOCK_SIZE = 10000;
//
//     for (int i = 0; i < BLOCK_SIZE; ++i) {
//         mutable_block.add_row(generate_row());
//     }
//
//     ASSERT_EQ(BLOCK_SIZE, mutable_block.rows());
//     Block new_block = mutable_block.to_block();
//     ASSERT_EQ(5, new_block.columns());
//     ASSERT_EQ(BLOCK_SIZE, new_block.rows());
//     mem_table->insert(std::move(new_block));
//     ASSERT_EQ(BLOCK_SIZE, mem_table->rows());
//     size_t flush_row_nums = 0;
//     mem_table->flush(&flush_row_nums);
//     ASSERT_EQ(BLOCK_SIZE, flush_row_nums);
// }
//
// TEST(MemTableTest, MultiMemTableTest) {
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::unique_ptr<MemTable> mem_table = std::make_unique<MemTable>(file_writer.get(), table_schema, 0);
//     Block block = table_schema->create_block();
//     MutableBlock mutable_block = MutableBlock::build_mutable_block(&block);
//
//     const size_t BLOCK_SIZE = 10000;
//
//     for (int i = 0; i < BLOCK_SIZE; ++i) {
//         Row row = generate_row();
//         mutable_block.add_row(row);
//         mutable_block.add_row(row);
//     }
//
//     ASSERT_EQ(BLOCK_SIZE * 2, mutable_block.rows());
//     Block new_block = mutable_block.to_block();
//     ASSERT_EQ(5, new_block.columns());
//     ASSERT_EQ(BLOCK_SIZE * 2, new_block.rows());
//     mem_table->insert(std::move(new_block));
//     ASSERT_EQ(BLOCK_SIZE, mem_table->rows());
//     size_t flush_row_nums = 0;
//     mem_table->flush(&flush_row_nums);
//     ASSERT_EQ(BLOCK_SIZE, flush_row_nums);
// }
//
// TEST(MemTableTest, ContentMemTableTest) {
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::unique_ptr<MemTable> mem_table = std::make_unique<MemTable>(file_writer.get(), table_schema, 0);
//     Block block1 = table_schema->create_block();
//     Block block2 = table_schema->create_block();
//     MutableBlock mutable_block = MutableBlock::build_mutable_block(&block1);
//     MutableBlock target_mutable_block = MutableBlock::build_mutable_block(&block2);
//
//     const size_t BLOCK_SIZE = 10000;
//
//     std::vector<Row> rows;
//
//     for (int i = 0; i < BLOCK_SIZE; ++i) {
//         Row row = generate_row();
//         mutable_block.add_row(row);
//         rows.emplace_back(row);
//     }
//
//     std::sort(rows.begin(), rows.end(), [](const Row& lhs, const Row& rhs) -> bool {
//         if (lhs.vin == rhs.vin) {
//             return lhs.timestamp < rhs.timestamp;
//         }
//         return lhs.vin < rhs.vin;
//     });
//
//     for (const auto& row : rows) {
//         target_mutable_block.add_row(row);
//     }
//
//     Block target_block = target_mutable_block.to_block();
//
//     ASSERT_EQ(BLOCK_SIZE, mutable_block.rows());
//     Block new_block = mutable_block.to_block();
//     ASSERT_EQ(5, new_block.columns());
//     ASSERT_EQ(BLOCK_SIZE, new_block.rows());
//     mem_table->insert(std::move(new_block));
//     ASSERT_EQ(BLOCK_SIZE, mem_table->rows());
//     size_t flush_row_nums = 0;
//     mem_table->flush(&flush_row_nums);
//     ASSERT_EQ(BLOCK_SIZE, flush_row_nums);
// }
//
// TEST(MemTableTest, HandleLatestQueryMemTableTest) {
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::unique_ptr<MemTable> mem_table = std::make_unique<MemTable>(file_writer.get(), table_schema, 0);
//     Block block = table_schema->create_block();
//     MutableBlock mutable_block = MutableBlock::build_mutable_block(&block);
//
//     std::vector<Row> rows;
//     std::unordered_map<std::string, std::priority_queue<int64_t>> maps;
//
//     const size_t DATA_SIZE = generate_dataset(rows, maps);
//
//     for (const auto& row : rows) {
//         mutable_block.add_row(row);
//     }
//
//     std::sort(rows.begin(), rows.end(), [](const Row& lhs, const Row& rhs) -> bool {
//         if (lhs.vin == rhs.vin) {
//             return lhs.timestamp < rhs.timestamp;
//         }
//         return lhs.vin < rhs.vin;
//     });
//
//     ASSERT_EQ(DATA_SIZE, mutable_block.rows());
//     Block new_block = mutable_block.to_block();
//     ASSERT_EQ(5, new_block.columns());
//     ASSERT_EQ(DATA_SIZE, new_block.rows());
//     mem_table->insert(std::move(new_block));
//     ASSERT_EQ(DATA_SIZE, mem_table->rows());
//
//     Block result_block = table_schema->create_block();
//
//     Vin input_vin = rows[generate_random_int32() % rows.size()].vin;
//     Row input_row = generate_row();
//     input_row.vin = generate_vin(increase_vin(input_vin));
//     input_row.timestamp = 0;
//
//     auto result = handle_latest_query(input_vin, maps);
//     mem_table->handle_latest_query({input_row}, &result_block);
//     ASSERT_EQ(result.has_value(), result_block.rows() == 1);
//
//     if (result.has_value()) {
//         const ColumnInt64& dst = reinterpret_cast<const ColumnInt64&>(*result_block.get_by_position(1)._column);
//         ASSERT_EQ(*result, dst.get(0));
//     }
// }
//
// TEST(MemTableTest, HandleTimeRangeQueryMemTableTest) {
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::unique_ptr<MemTable> mem_table = std::make_unique<MemTable>(file_writer.get(), table_schema, 0);
//     Block block = table_schema->create_block();
//     MutableBlock mutable_block = MutableBlock::build_mutable_block(&block);
//
//     std::vector<Row> rows;
//     std::unordered_map<std::string, std::priority_queue<int64_t>> maps;
//
//     const size_t DATA_SIZE = generate_dataset(rows, maps);
//
//     for (const auto& row : rows) {
//         mutable_block.add_row(row);
//     }
//
//     std::sort(rows.begin(), rows.end(), [](const Row& lhs, const Row& rhs) -> bool {
//         if (lhs.vin == rhs.vin) {
//             return lhs.timestamp < rhs.timestamp;
//         }
//         return lhs.vin < rhs.vin;
//     });
//
//     ASSERT_EQ(DATA_SIZE, mutable_block.rows());
//     Block new_block = mutable_block.to_block();
//     ASSERT_EQ(5, new_block.columns());
//     ASSERT_EQ(DATA_SIZE, new_block.rows());
//     mem_table->insert(std::move(new_block));
//     ASSERT_EQ(DATA_SIZE, mem_table->rows());
//
//     Block result_block = table_schema->create_block();
//
//     int64_t rand_timestamp = generate_random_timestamp();
//     int64_t time_lower_bound = rand_timestamp / 2;
//     int64_t time_upper_bound = rand_timestamp;
//
//     Vin input_vin = rows[generate_random_int32() % rows.size()].vin;
//     Row lower_bound_row = generate_row();
//     lower_bound_row.vin = input_vin;
//     lower_bound_row.timestamp = time_lower_bound;
//     Row upper_bound_row = generate_row();
//     upper_bound_row.vin = input_vin;
//     upper_bound_row.timestamp = time_upper_bound;
//
//     auto result = handle_time_range_query(input_vin, time_lower_bound, time_upper_bound, maps);
//     mem_table->handle_time_range_query(lower_bound_row, upper_bound_row, &result_block);
//     ASSERT_EQ(result.has_value(), result_block.rows() > 0);
//
//     if (result.has_value()) {
//         std::vector<int64_t> results = *result;
//         const ColumnInt64& dst = reinterpret_cast<const ColumnInt64&>(*result_block.get_by_position(1)._column);
//         ASSERT_EQ(results.size(), dst.size());
//         std::sort(results.begin(), results.end());
//
//         for (size_t i = 0; i < dst.size(); ++i) {
//             ASSERT_EQ(results[i], dst.get(i));
//         }
//     }
// }
//
// TEST(MemTableTest, HandleEmptyLatestQueryMemTableTest) {
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::unique_ptr<MemTable> mem_table = std::make_unique<MemTable>(file_writer.get(), table_schema, 0);
//     Block block = table_schema->create_block();
//     MutableBlock mutable_block = MutableBlock::build_mutable_block(&block);
//
//     std::vector<Row> rows;
//     std::unordered_map<std::string, std::priority_queue<int64_t>> maps;
//
//     const size_t DATA_SIZE = generate_dataset(rows, maps);
//
//     for (const auto& row : rows) {
//         mutable_block.add_row(row);
//     }
//
//     std::sort(rows.begin(), rows.end(), [](const Row& lhs, const Row& rhs) -> bool {
//         if (lhs.vin == rhs.vin) {
//             return lhs.timestamp < rhs.timestamp;
//         }
//         return lhs.vin < rhs.vin;
//     });
//
//     ASSERT_EQ(DATA_SIZE, mutable_block.rows());
//     Block new_block = mutable_block.to_block();
//     ASSERT_EQ(5, new_block.columns());
//     ASSERT_EQ(DATA_SIZE, new_block.rows());
//     mem_table->insert(std::move(new_block));
//     // ASSERT_EQ(DATA_SIZE, mem_table->rows());
//
//     Block result_block = table_schema->create_block();
//
//     Row input_row = generate_row();
//     input_row.timestamp = 0;
//
//     auto result = handle_latest_query(input_row.vin, maps);
//     mem_table->handle_latest_query({input_row}, &result_block);
//     ASSERT_EQ(!result.has_value(), result_block.rows() == 0);
// }
//
// TEST(MemTableTest, HandleEmptyTimeRangeQueryMemTableTest) {
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(schema);
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::unique_ptr<MemTable> mem_table = std::make_unique<MemTable>(file_writer.get(), table_schema, 0);
//     Block block = table_schema->create_block();
//     MutableBlock mutable_block = MutableBlock::build_mutable_block(&block);
//
//     std::vector<Row> rows;
//     std::unordered_map<std::string, std::priority_queue<int64_t>> maps;
//
//     const size_t DATA_SIZE = generate_dataset(rows, maps);
//
//     for (const auto& row : rows) {
//         mutable_block.add_row(row);
//     }
//
//     std::sort(rows.begin(), rows.end(), [](const Row& lhs, const Row& rhs) -> bool {
//         if (lhs.vin == rhs.vin) {
//             return lhs.timestamp < rhs.timestamp;
//         }
//         return lhs.vin < rhs.vin;
//     });
//
//     ASSERT_EQ(DATA_SIZE, mutable_block.rows());
//     Block new_block = mutable_block.to_block();
//     ASSERT_EQ(5, new_block.columns());
//     ASSERT_EQ(DATA_SIZE, new_block.rows());
//     mem_table->insert(std::move(new_block));
//     ASSERT_EQ(DATA_SIZE, mem_table->rows());
//
//     Block result_block = table_schema->create_block();
//
//     int64_t time_lower_bound = 0;
//     int64_t time_upper_bound = 1;
//
//     Vin input_vin = rows[generate_random_int32() % rows.size()].vin;
//     Row lower_bound_row = generate_row();
//     lower_bound_row.vin = input_vin;
//     lower_bound_row.timestamp = time_lower_bound;
//     Row upper_bound_row = generate_row();
//     upper_bound_row.vin = input_vin;
//     upper_bound_row.timestamp = time_upper_bound;
//
//     auto result = handle_time_range_query(input_vin, time_lower_bound, time_upper_bound, maps);
//     mem_table->handle_time_range_query(lower_bound_row, upper_bound_row, &result_block);
//     ASSERT_EQ(!result.has_value(), result_block.rows() == 0);
// }
//
// }
//
