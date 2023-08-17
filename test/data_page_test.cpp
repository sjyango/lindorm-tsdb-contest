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
//
// #include <gtest/gtest.h>
//
// #include "storage/page_encoder.h"
// #include "storage/page_decoder.h"
// #include "vec/columns/ColumnString.h"
// #include "common/data_type_factory.h"
//
// namespace LindormContest::test {
//
// using namespace storage;
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
// TEST(PageTest, Int32PlainPageTest) {
//     const size_t N = 10000;
//     auto type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_INTEGER);
//     PlainPageEncoder encoder(type->type_size());
//     PlainPageDecoder decoder(type);
//     std::vector<int32_t> nums;
//     vectorized::MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_INTEGER, "dst");
//
//     for (size_t i = 0; i < N; ++i) {
//         nums.push_back(generate_random_int32());
//     }
//
//     size_t write_count = N;
//     encoder.add(reinterpret_cast<const UInt8*>(nums.data()), &write_count);
//     ASSERT_EQ(write_count, N);
//     OwnedSlice data = encoder.finish();
//     decoder.init(data.slice());
//     decoder.seek_to_position_in_page(0);
//     size_t read_count = N;
//     decoder.next_batch(&read_count, dst);
//     ASSERT_EQ(read_count, N);
//     ASSERT_EQ(dst->size(), N);
//     const vectorized::ColumnInt32& int32_col = reinterpret_cast<const vectorized::ColumnInt32&>(*dst);
//
//     for (size_t i = 0; i < N; ++i) {
//         ASSERT_EQ(int32_col.get(i), nums[i]);
//     }
// }
//
// TEST(PageTest, FullInt32PlainPageTest) {
//     const size_t N = 300000;
//     auto type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_INTEGER);
//     PlainPageEncoder encoder(type->type_size());
//     PlainPageDecoder decoder(type);
//     std::vector<int32_t> nums;
//     vectorized::MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_INTEGER, "dst");
//
//     for (size_t i = 0; i < N; ++i) {
//         nums.push_back(generate_random_int32());
//     }
//
//     size_t write_count = N;
//     encoder.add(reinterpret_cast<const UInt8*>(nums.data()), &write_count);
//     ASSERT_LT(write_count, N);
//     OwnedSlice data = encoder.finish();
//     decoder.init(data.slice());
//     decoder.seek_to_position_in_page(0);
//     size_t read_count = N;
//     decoder.next_batch(&read_count, dst);
//     ASSERT_LT(read_count, N);
//     ASSERT_EQ(dst->size(), read_count);
//     ASSERT_EQ(write_count, read_count);
//     const vectorized::ColumnInt32& int32_col = reinterpret_cast<const vectorized::ColumnInt32&>(*dst);
//
//     for (size_t i = 0; i < read_count; ++i) {
//         ASSERT_EQ(int32_col.get(i), nums[i]);
//     }
// }
//
// TEST(PageTest, Int64PlainPageTest) {
//     const size_t N = 10000;
//     auto type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_TIMESTAMP);
//     PlainPageEncoder encoder(type->type_size());
//     PlainPageDecoder decoder(type);
//     std::vector<int64_t> nums;
//     vectorized::MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_TIMESTAMP, "dst");
//
//     for (size_t i = 0; i < N; ++i) {
//         nums.push_back(generate_random_timestamp());
//     }
//
//     size_t write_count = N;
//     encoder.add(reinterpret_cast<const UInt8*>(nums.data()), &write_count);
//     ASSERT_EQ(write_count, N);
//     OwnedSlice data = encoder.finish();
//     decoder.init(data.slice());
//     decoder.seek_to_position_in_page(0);
//     size_t read_count = N;
//     decoder.next_batch(&read_count, dst);
//     ASSERT_EQ(read_count, N);
//     ASSERT_EQ(dst->size(), N);
//     const vectorized::ColumnInt64& int64_col = reinterpret_cast<const vectorized::ColumnInt64&>(*dst);
//
//     for (size_t i = 0; i < N; ++i) {
//         ASSERT_EQ(int64_col.get(i), nums[i]);
//     }
// }
//
// TEST(PageTest, FullInt64PlainPageTest) {
//     const size_t N = 300000;
//     auto type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_TIMESTAMP);
//     PlainPageEncoder encoder(type->type_size());
//     PlainPageDecoder decoder(type);
//     std::vector<int64_t> nums;
//     vectorized::MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_TIMESTAMP, "dst");
//
//     for (size_t i = 0; i < N; ++i) {
//         nums.push_back(generate_random_timestamp());
//     }
//
//     size_t write_count = N;
//     encoder.add(reinterpret_cast<const UInt8*>(nums.data()), &write_count);
//     ASSERT_LT(write_count, N);
//     OwnedSlice data = encoder.finish();
//     decoder.init(data.slice());
//     decoder.seek_to_position_in_page(0);
//     size_t read_count = N;
//     decoder.next_batch(&read_count, dst);
//     ASSERT_LT(read_count, N);
//     ASSERT_EQ(dst->size(), read_count);
//     ASSERT_EQ(write_count, read_count);
//     const vectorized::ColumnInt64& int64_col = reinterpret_cast<const vectorized::ColumnInt64&>(*dst);
//
//     for (size_t i = 0; i < read_count; ++i) {
//         ASSERT_EQ(int64_col.get(i), nums[i]);
//     }
// }
//
// TEST(PageTest, Float64PlainPageTest) {
//     const size_t N = 10000;
//     auto type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_DOUBLE_FLOAT);
//     PlainPageEncoder encoder(type->type_size());
//     PlainPageDecoder decoder(type);
//     std::vector<double_t> nums;
//     vectorized::MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_DOUBLE_FLOAT, "dst");
//
//     for (size_t i = 0; i < N; ++i) {
//         nums.push_back(generate_random_float64());
//     }
//
//     size_t write_count = N;
//     encoder.add(reinterpret_cast<const UInt8*>(nums.data()), &write_count);
//     ASSERT_EQ(write_count, N);
//     OwnedSlice data = encoder.finish();
//     decoder.init(data.slice());
//     decoder.seek_to_position_in_page(0);
//     size_t read_count = N;
//     decoder.next_batch(&read_count, dst);
//     ASSERT_EQ(read_count, N);
//     ASSERT_EQ(dst->size(), N);
//     const vectorized::ColumnFloat64& float64_col = reinterpret_cast<const vectorized::ColumnFloat64&>(*dst);
//
//     for (size_t i = 0; i < N; ++i) {
//         ASSERT_EQ(float64_col.get(i), nums[i]);
//     }
// }
//
// TEST(PageTest, FullFloat64PlainPageTest) {
//     const size_t N = 300000;
//     auto type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_DOUBLE_FLOAT);
//     PlainPageEncoder encoder(type->type_size());
//     PlainPageDecoder decoder(type);
//     std::vector<double_t> nums;
//     vectorized::MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_DOUBLE_FLOAT, "dst");
//
//     for (size_t i = 0; i < N; ++i) {
//         nums.push_back(generate_random_float64());
//     }
//
//     size_t write_count = N;
//     encoder.add(reinterpret_cast<const UInt8*>(nums.data()), &write_count);
//     ASSERT_LT(write_count, N);
//     OwnedSlice data = encoder.finish();
//     decoder.init(data.slice());
//     decoder.seek_to_position_in_page(0);
//     size_t read_count = N;
//     decoder.next_batch(&read_count, dst);
//     ASSERT_LT(read_count, N);
//     ASSERT_EQ(dst->size(), read_count);
//     ASSERT_EQ(write_count, read_count);
//     const vectorized::ColumnFloat64& float64_col = reinterpret_cast<const vectorized::ColumnFloat64&>(*dst);
//
//     for (size_t i = 0; i < read_count; ++i) {
//         ASSERT_EQ(float64_col.get(i), nums[i]);
//     }
// }
//
// TEST(PageTest, BinaryPlainPageTest) {
//     const size_t N = 10000;
//     BinaryPlainPageEncoder encoder;
//     BinaryPlainPageDecoder decoder;
//     std::vector<std::string> ss;
//     std::vector<Slice> slices;
//     vectorized::MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_STRING, "dst");
//
//     for (size_t i = 0; i < N; ++i) {
//         std::string s = generate_random_string(1000);
//         ss.push_back(std::move(s));
//         slices.push_back(ss.back());
//     }
//
//     size_t write_count = N;
//     encoder.add(reinterpret_cast<const uint8_t*>(slices.data()), &write_count);
//     ASSERT_EQ(write_count, N);
//     OwnedSlice data = encoder.finish();
//     decoder.init(data.slice());
//     decoder.seek_to_position_in_page(0);
//     size_t read_count = N;
//     decoder.next_batch(&read_count, dst);
//     ASSERT_EQ(read_count, N);
//     ASSERT_EQ(dst->size(), N);
//     const vectorized::ColumnString& str_col = reinterpret_cast<const vectorized::ColumnString&>(*dst);
//
//     for (size_t i = 0; i < N; ++i) {
//         ASSERT_EQ(str_col.get(i), ss[i]);
//     }
// }
//
// TEST(PageTest, FullBinaryPlainPageTest) {
//     const size_t N = 10000;
//     BinaryPlainPageEncoder encoder;
//     BinaryPlainPageDecoder decoder;
//     std::vector<std::string> ss;
//     std::vector<Slice> slices;
//     vectorized::MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_STRING, "dst");
//
//     for (size_t i = 0; i < N; ++i) {
//         std::string s = generate_random_string(2000);
//         ss.push_back(std::move(s));
//         slices.push_back(ss.back());
//     }
//
//     size_t write_count = N;
//     encoder.add(reinterpret_cast<const uint8_t*>(slices.data()), &write_count);
//     ASSERT_LT(write_count, N);
//     OwnedSlice data = encoder.finish();
//     decoder.init(data.slice());
//     decoder.seek_to_position_in_page(0);
//     size_t read_count = N;
//     decoder.next_batch(&read_count, dst);
//     ASSERT_LT(read_count, N);
//     ASSERT_EQ(dst->size(), read_count);
//     ASSERT_EQ(write_count, read_count);
//     const vectorized::ColumnString& str_col = reinterpret_cast<const vectorized::ColumnString&>(*dst);
//
//     for (size_t i = 0; i < read_count; ++i) {
//         ASSERT_EQ(str_col.get(i), ss[i]);
//     }
// }
//
// }
//
