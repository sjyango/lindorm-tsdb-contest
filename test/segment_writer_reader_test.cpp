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
// #include <random>
// #include <chrono>
//
// #include <gtest/gtest.h>
//
// #include "TSDBEngineImpl.h"
//
// namespace LindormContest::test {
//
// using namespace storage;
// using namespace vectorized;
//
// static std::string generate_random_string(int length) {
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
// static int32_t generate_random_int32() {
//     std::random_device rd;
//     std::mt19937_64 gen(rd());
//     std::uniform_int_distribution<int32_t> dis(0, std::numeric_limits<int32_t>::max());
//     return dis(gen);
// }
//
// static int64_t generate_random_timestamp() {
//     std::random_device rd;
//     std::mt19937_64 gen(rd());
//     std::uniform_int_distribution<int64_t> dis(0, std::numeric_limits<int64_t>::max());
//     return dis(gen);
// }
//
// static double_t generate_random_float64() {
//     std::random_device rd;
//     std::mt19937 gen(rd());
//     std::uniform_real_distribution<double> dis(0.0, 1.0);
//     return dis(gen);
// }
//
// static io::FileWriterPtr generate_file_writer() {
//     io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
//     io::Path file_path = root_path / io::Path("segment_writer_reader_test.dat");
//     io::FileSystemSPtr fs = io::FileSystem::create(root_path);
//     if (fs->exists(file_path)) {
//         fs->delete_file(file_path);
//     }
//     assert(!fs->exists(file_path));
//     return fs->create_file(file_path);
// }
//
// static io::FileReaderSPtr generate_file_reader() {
//     io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
//     io::Path file_path = root_path / io::Path("segment_writer_reader_test.dat");
//     io::FileSystemSPtr fs = io::FileSystem::create(root_path);
//     assert(fs->exists(file_path));
//     return fs->open_file(file_path);
// }
//
// static Row generate_row() {
//     std::map<std::string, ColumnValue> columns;
//     ColumnValue col2_val(generate_random_string(20));
//     columns.insert({"col2", col2_val});
//     ColumnValue col3_val(generate_random_float64());
//     columns.insert({"col3", col3_val});
//     ColumnValue col4_val(generate_random_int32());
//     columns.insert({"col4", col4_val});
//     Row row;
//     row.vin = Vin(generate_random_string(17));
//     row.timestamp = generate_random_timestamp();
//     row.columns = std::move(columns);
//     return row;
// }
//
// static bool compare_rows(const Row& lhs, const Row& rhs) {
//     bool res = (lhs.vin == rhs.vin) && (lhs.timestamp == rhs.timestamp);
//     assert(lhs.columns.size() == rhs.columns.size());
//     for (const auto& item : lhs.columns) {
//         switch (item.second.columnType) {
//         case COLUMN_TYPE_INTEGER: {
//             int32_t lhs_val;
//             int32_t rhs_val;
//             item.second.getIntegerValue(lhs_val);
//             auto it = rhs.columns.find(item.first);
//             assert(it != rhs.columns.end());
//             it->second.getIntegerValue(rhs_val);
//             res = res && (lhs_val == rhs_val);
//             break;
//         }
//         case COLUMN_TYPE_DOUBLE_FLOAT: {
//             double_t lhs_val;
//             double_t rhs_val;
//             item.second.getDoubleFloatValue(lhs_val);
//             auto it = rhs.columns.find(item.first);
//             assert(it != rhs.columns.end());
//             it->second.getDoubleFloatValue(rhs_val);
//             res = res && (lhs_val == rhs_val);
//             break;
//         }
//         case COLUMN_TYPE_STRING: {
//             std::pair<int32_t, const char *> lhs_val;
//             std::pair<int32_t, const char *> rhs_val;
//             item.second.getStringValue(lhs_val);
//             auto it = rhs.columns.find(item.first);
//             assert(it != rhs.columns.end());
//             it->second.getStringValue(rhs_val);
//             res = res && (lhs_val.first == rhs_val.first) && (std::strncmp(lhs_val.second, rhs_val.second, lhs_val.first) == 0);
//             break;
//         }
//         default:
//             return false;
//         }
//     }
//     return res;
// }
//
// TEST(SegmentWriterReaderTest, BasicSegmentWriterReaderTest) {
//     const size_t N = 10000;
//     const size_t SEGMENT_ID = 0;
//     // ######################################## SegmentWriter ########################################
//
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     TableSchemaSPtr table_schema = std::make_shared<TableSchema>(schema);
//     std::unique_ptr<SegmentWriter> segment_writer = std::make_unique<SegmentWriter>(file_writer.get(), table_schema, SEGMENT_ID);
//     vectorized::Block block = table_schema->create_block();
//     vectorized::MutableBlock src_block = MutableBlock::build_mutable_block(&block);
//     std::vector<Row> src_rows;
//
//     for (size_t i = 0; i < N; ++i) {
//         src_rows.emplace_back(generate_row());
//     }
//
//     std::sort(src_rows.begin(), src_rows.end(), [] (const Row& lhs, const Row& rhs) {
//         if (lhs.vin != rhs.vin) {
//             return lhs.vin < rhs.vin;
//         }
//         return lhs.timestamp < rhs.timestamp;
//     });
//
//     for (const auto& row : src_rows) {
//         src_block.add_row(row);
//     }
//
//     ASSERT_EQ(src_block.rows(), N);
//     size_t num_written;
//
//     auto write_start = std::chrono::high_resolution_clock::now();
//
//     segment_writer->append_block(std::move(src_block.to_block()), &num_written);
//     segment_writer->finalize();
//     ASSERT_EQ(num_written, N);
//     file_writer->finalize();
//     file_writer->close();
//
//     auto write_end = std::chrono::high_resolution_clock::now();
//     auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(write_end - write_start);
//     INFO_LOG("write costs %ld ms", write_duration.count())
//
//     // ######################################## SegmentWriter ########################################
//
//     // ######################################## SegmentReader ########################################
//
//     io::FileReaderSPtr file_reader = generate_file_reader();
//     io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
//     io::Path segment_path = root_path / io::Path("segment_writer_reader_test.dat");
//     PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table_schema);
//
//     auto read_start = std::chrono::high_resolution_clock::now();
//
//     std::unique_ptr<SegmentReader> segment_reader = std::make_unique<SegmentReader>(SEGMENT_ID, file_reader, table_schema, partial_schema);
//     vectorized::Block dst_block = table_schema->create_block();
//     size_t num_to_read = N;
//     segment_reader->seek_to_first();
//     segment_reader->next_batch(&num_to_read, &dst_block);
//     ASSERT_EQ(num_to_read, N);
//
//     auto read_end = std::chrono::high_resolution_clock::now();
//     auto read_duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_end - read_start);
//     INFO_LOG("read costs %ld ms", read_duration.count())
//
//     // ######################################## SegmentReader ########################################
//
//     std::vector<Row> dst_rows = dst_block.to_rows(0, N);
//
//     for (size_t i = 0; i < N; ++i) {
//         ASSERT_TRUE(compare_rows(src_rows[i], dst_rows[i]));
//     }
// }
//
// TEST(SegmentWriterReaderTest, SeekSegmentWriterReaderTest) {
//     const size_t N = 10000;
//     const size_t SEGMENT_ID = 0;
//     // ######################################## SegmentWriter ########################################
//
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     TableSchemaSPtr table_schema = std::make_shared<TableSchema>(schema);
//     std::unique_ptr<SegmentWriter> segment_writer = std::make_unique<SegmentWriter>(file_writer.get(), table_schema, SEGMENT_ID);
//     vectorized::Block block = table_schema->create_block();
//     vectorized::MutableBlock src_block = MutableBlock::build_mutable_block(&block);
//     std::vector<Row> src_rows;
//
//     for (size_t i = 0; i < N; ++i) {
//         src_rows.emplace_back(generate_row());
//     }
//
//     std::sort(src_rows.begin(), src_rows.end(), [] (const Row& lhs, const Row& rhs) {
//         if (lhs.vin != rhs.vin) {
//             return lhs.vin < rhs.vin;
//         }
//         return lhs.timestamp < rhs.timestamp;
//     });
//
//     for (const auto& row : src_rows) {
//         src_block.add_row(row);
//     }
//
//     ASSERT_EQ(src_block.rows(), N);
//     size_t num_written;
//
//     auto write_start = std::chrono::high_resolution_clock::now();
//
//     segment_writer->append_block(std::move(src_block.to_block()), &num_written);
//     segment_writer->finalize();
//     ASSERT_EQ(num_written, N);
//     file_writer->finalize();
//     file_writer->close();
//
//     auto write_end = std::chrono::high_resolution_clock::now();
//     auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(write_end - write_start);
//     INFO_LOG("write costs %ld ms", write_duration.count())
//
//     // ######################################## SegmentWriter ########################################
//
//     // ######################################## SegmentReader ########################################
//
//     io::FileReaderSPtr file_reader = generate_file_reader();
//     io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
//     io::Path segment_path = root_path / io::Path("segment_writer_reader_test.dat");
//     PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table_schema);
//
//     auto read_start = std::chrono::high_resolution_clock::now();
//
//     std::unique_ptr<SegmentReader> segment_reader = std::make_unique<SegmentReader>(SEGMENT_ID, file_reader, table_schema, partial_schema);
//     vectorized::Block dst_block = table_schema->create_block();
//     size_t rand_ordinal = generate_random_int32() % N;
//     size_t num_to_read = rand_ordinal / 10;
//     segment_reader->seek_to_ordinal(rand_ordinal);
//     segment_reader->next_batch(&num_to_read, &dst_block);
//     ASSERT_EQ(num_to_read, std::min(N - rand_ordinal, rand_ordinal / 10));
//     ASSERT_EQ(dst_block.rows(), num_to_read);
//
//     auto read_end = std::chrono::high_resolution_clock::now();
//     auto read_duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_end - read_start);
//     INFO_LOG("read costs %ld ms", read_duration.count())
//
//     // ######################################## SegmentReader ########################################
//
//     std::vector<Row> dst_rows = dst_block.to_rows(0, num_to_read);
//
//     for (size_t i = 0; i < num_to_read; ++i) {
//         ASSERT_TRUE(compare_rows(src_rows[i + rand_ordinal], dst_rows[i]));
//     }
// }
//
// TEST(SegmentWriterReaderTest, SegmentLatestQueryTest) {
//     const size_t N = 10000;
//     const size_t SEGMENT_ID = 0;
//     // ######################################## SegmentWriter ########################################
//
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     TableSchemaSPtr table_schema = std::make_shared<TableSchema>(schema);
//     std::unique_ptr<SegmentWriter> segment_writer = std::make_unique<SegmentWriter>(file_writer.get(), table_schema, SEGMENT_ID);
//     vectorized::Block block = table_schema->create_block();
//     vectorized::MutableBlock src_block = MutableBlock::build_mutable_block(&block);
//     std::vector<Row> src_rows;
//
//     for (size_t i = 0; i < N; ++i) {
//         src_rows.emplace_back(generate_row());
//     }
//
//     std::sort(src_rows.begin(), src_rows.end(), [] (const Row& lhs, const Row& rhs) {
//         if (lhs.vin != rhs.vin) {
//             return lhs.vin < rhs.vin;
//         }
//         return lhs.timestamp < rhs.timestamp;
//     });
//
//     for (const auto& row : src_rows) {
//         src_block.add_row(row);
//     }
//
//     ASSERT_EQ(src_block.rows(), N);
//     size_t num_written;
//
//     auto write_start = std::chrono::high_resolution_clock::now();
//
//     segment_writer->append_block(std::move(src_block.to_block()), &num_written);
//     segment_writer->finalize();
//     ASSERT_EQ(num_written, N);
//     file_writer->finalize();
//     file_writer->close();
//
//     auto write_end = std::chrono::high_resolution_clock::now();
//     auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(write_end - write_start);
//     INFO_LOG("write costs %ld ms", write_duration.count())
//
//     // ######################################## SegmentWriter ########################################
//
//     // ######################################## SegmentReader ########################################
//
//     io::FileReaderSPtr file_reader = generate_file_reader();
//     io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
//     io::Path segment_path = root_path / io::Path("segment_writer_reader_test.dat");
//     PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table_schema);
//
//     std::unique_ptr<SegmentReader> segment_reader = std::make_unique<SegmentReader>(SEGMENT_ID, file_reader, table_schema, partial_schema);
//     vectorized::Block dst_block = table_schema->create_block();
//     size_t rand_ordinal = generate_random_int32() % src_rows.size();
//     Vin rand_vin = src_rows[rand_ordinal].vin;
//     auto result = segment_reader->handle_latest_query(rand_vin);
//     ASSERT_TRUE(result.has_value());
//     while (src_rows[rand_ordinal].vin == rand_vin) {
//         rand_ordinal++;
//     }
//     ASSERT_EQ((*result).timestamp, src_rows[rand_ordinal - 1].timestamp);
//
//     // ######################################## SegmentReader ########################################
// }
//
// TEST(SegmentWriterReaderTest, SegmentTimeRangeQueryTest) {
//     const size_t N = 10000;
//     const size_t SEGMENT_ID = 0;
//     // ######################################## SegmentWriter ########################################
//
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     TableSchemaSPtr table_schema = std::make_shared<TableSchema>(schema);
//     std::unique_ptr<SegmentWriter> segment_writer = std::make_unique<SegmentWriter>(file_writer.get(), table_schema, SEGMENT_ID);
//     vectorized::Block block = table_schema->create_block();
//     vectorized::MutableBlock src_block = MutableBlock::build_mutable_block(&block);
//     std::vector<Row> src_rows;
//
//     for (size_t i = 0; i < N; ++i) {
//         src_rows.emplace_back(generate_row());
//     }
//
//     std::sort(src_rows.begin(), src_rows.end(), [] (const Row& lhs, const Row& rhs) {
//         if (lhs.vin != rhs.vin) {
//             return lhs.vin < rhs.vin;
//         }
//         return lhs.timestamp < rhs.timestamp;
//     });
//
//     for (const auto& row : src_rows) {
//         src_block.add_row(row);
//     }
//
//     ASSERT_EQ(src_block.rows(), N);
//     size_t num_written;
//     segment_writer->append_block(std::move(src_block.to_block()), &num_written);
//     segment_writer->finalize();
//     ASSERT_EQ(num_written, N);
//     file_writer->finalize();
//     file_writer->close();
//
//     // ######################################## SegmentWriter ########################################
//
//     // ######################################## SegmentReader ########################################
//
//     io::FileReaderSPtr file_reader = generate_file_reader();
//     io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
//     io::Path segment_path = root_path / io::Path("segment_writer_reader_test.dat");
//     PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table_schema);
//
//     std::unique_ptr<SegmentReader> segment_reader = std::make_unique<SegmentReader>(SEGMENT_ID, file_reader, table_schema, partial_schema);
//     vectorized::Block dst_block = table_schema->create_block();
//     size_t rand_ordinal = generate_random_int32() % src_rows.size();
//     size_t rand_timestamp = generate_random_timestamp();
//     Vin rand_vin = src_rows[rand_ordinal].vin;
//     std::vector<Row> query_results;
//     std::vector<Row> ground_truths;
//
//     for (const auto& row : src_rows) {
//         if (row.vin == rand_vin && row.timestamp >= 0 && row.timestamp < rand_timestamp) {
//             std::string s(row.vin.vin, 17);
//             ground_truths.emplace_back(row);
//         }
//     }
//
//     auto result = segment_reader->handle_time_range_query(rand_vin, 0, rand_timestamp);
//     if (result.has_value()) {
//         query_results = (*result).to_rows();
//     }
//
//     ASSERT_EQ(query_results, ground_truths);
//
//     // ######################################## SegmentReader ########################################
// }
//
// TEST(SegmentWriterReaderTest, DISABLED_SegmentWriterReaderBenchmarkTest) {
//     const size_t N = 1000000;
//     const size_t SEGMENT_ID = 0;
//     // ######################################## SegmentWriter ########################################
//
//     io::FileWriterPtr file_writer = generate_file_writer();
//     std::map<std::string, ColumnType> columnTypeMap;
//     columnTypeMap.insert({"col2", COLUMN_TYPE_STRING});
//     columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
//     columnTypeMap.insert({"col4", COLUMN_TYPE_INTEGER});
//     Schema schema;
//     schema.columnTypeMap = std::move(columnTypeMap);
//     TableSchemaSPtr table_schema = std::make_shared<TableSchema>(schema);
//     std::unique_ptr<SegmentWriter> segment_writer = std::make_unique<SegmentWriter>(file_writer.get(), table_schema, SEGMENT_ID);
//     vectorized::Block block = table_schema->create_block();
//     vectorized::MutableBlock src_block = MutableBlock::build_mutable_block(&block);
//     std::vector<Row> src_rows;
//
//     for (size_t i = 0; i < N; ++i) {
//         src_rows.emplace_back(generate_row());
//     }
//
//     std::sort(src_rows.begin(), src_rows.end(), [] (const Row& lhs, const Row& rhs) {
//         if (lhs.vin != rhs.vin) {
//             return lhs.vin < rhs.vin;
//         }
//         return lhs.timestamp < rhs.timestamp;
//     });
//
//     for (const auto& row : src_rows) {
//         src_block.add_row(row);
//     }
//
//     ASSERT_EQ(src_block.rows(), N);
//     size_t num_written;
//
//     auto write_start = std::chrono::high_resolution_clock::now();
//
//     segment_writer->append_block(std::move(src_block.to_block()), &num_written);
//     segment_writer->finalize();
//     ASSERT_EQ(num_written, N);
//     file_writer->finalize();
//     file_writer->close();
//
//     auto write_end = std::chrono::high_resolution_clock::now();
//     auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(write_end - write_start);
//     INFO_LOG("write costs %ld ms", write_duration.count())
//
//     // ######################################## SegmentWriter ########################################
//
//     // ######################################## SegmentReader ########################################
//
//     io::FileReaderSPtr file_reader = generate_file_reader();
//     io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
//     io::Path segment_path = root_path / io::Path("segment_writer_reader_test.dat");
//     PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table_schema);
//
//     auto read_start = std::chrono::high_resolution_clock::now();
//
//     std::unique_ptr<SegmentReader> segment_reader = std::make_unique<SegmentReader>(SEGMENT_ID, file_reader, table_schema, partial_schema);
//     vectorized::Block dst_block = table_schema->create_block();
//     size_t num_to_read = N;
//     segment_reader->seek_to_first();
//     segment_reader->next_batch(&num_to_read, &dst_block);
//     ASSERT_EQ(num_to_read, N);
//
//     auto read_end = std::chrono::high_resolution_clock::now();
//     auto read_duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_end - read_start);
//     INFO_LOG("read costs %ld ms", read_duration.count())
//
//     // ######################################## SegmentReader ########################################
// }
//
// }