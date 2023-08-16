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
// #include <unordered_set>
// #include <future>
// #include <chrono>
//
// #include <gtest/gtest.h>
// #include <random>
//
// #include "utils.h"
//
// namespace LindormContest::test {
//
// static bool order_func(const RowPosition& lhs, const RowPosition& rhs) {
//     if (lhs._vin != rhs._vin) {
//         return lhs._vin < rhs._vin;
//     }
//     return lhs._timestamp < rhs._timestamp;
// }
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
// static std::vector<std::vector<RowPosition>> generate_test_dataset() {
//     const size_t N = 10000;
//     std::vector<std::vector<RowPosition>> results;
//
//     for (size_t i = 0; i < 20; ++i) {
//         std::vector<RowPosition> result;
//         size_t rand_ordinal = generate_random_int32() % 1000;
//         for (size_t j = rand_ordinal; j < rand_ordinal + N; ++j) {
//             RowPosition row;
//             row._segment_id = i;
//             row._vin = std::to_string(generate_random_int32() % 100);
//             row._timestamp = generate_random_timestamp() % 100;
//             row._ordinal = j;
//             result.push_back(row);
//         }
//         std::sort(result.begin(), result.end(), order_func);
//         results.push_back(result);
//     }
//
//     return results;
// }
//
// static std::vector<RowPosition> map_record_method(std::vector<std::vector<RowPosition>>& dataset) {
//     std::unordered_set<RowPosition, RowPosition::HashFunc> merged_data;
//
//     for (auto it = dataset.rbegin(); it != dataset.rend(); ++it) {
//         const std::vector<RowPosition>& arr = *it;
//
//         for (auto& row : arr) {
//             if (merged_data.find(row) == merged_data.end()) {
//                 merged_data.insert(row);
//             }
//         }
//     }
//
//     std::vector<RowPosition> result;
//
//     for (const auto& row : merged_data) {
//         result.push_back(row);
//     }
//
//     return result;
// }
//
// static std::vector<RowPosition> merge_two_results(const std::vector<RowPosition>& old_dataset,
//                                                   const std::vector<RowPosition>& new_dataset) {
//     std::vector<RowPosition> result;
//     std::unordered_set<RowPosition, RowPosition::HashFunc> added;
//
//     for (auto& val : new_dataset) {
//         if (added.find(val) == added.end()) {
//             result.push_back(val);
//             added.insert(val);
//         }
//     }
//
//     for (auto& val : old_dataset) {
//         if (added.find(val) == added.end()) {
//             result.push_back(val);
//             added.insert(val);
//         }
//     }
//
//     return result;
// }
//
// static std::vector<RowPosition> merge_join_method(std::vector<std::vector<RowPosition>>& dataset) {
//     std::vector<RowPosition> merged_result;
//
//     for (const auto& item : dataset) {
//         merged_result = std::move(merge_two_results(merged_result, item));
//     }
//
//     return merged_result;
// }
//
// static std::vector<RowPosition> parallel_merge_join_method(std::vector<std::vector<RowPosition>> dataset) {
//     std::vector<RowPosition> merged_result;
//
//     if (dataset.size() == 0) {
//         return {};
//     }
//
//     if (dataset.size() == 1) {
//         return dataset[0];
//     }
//
//     auto mid = dataset.size() / 2;
//     auto it = dataset.begin();
//     std::advance(it, mid);
//     std::vector<std::vector<RowPosition>> first_half(dataset.begin(), it);
//     std::vector<std::vector<RowPosition>> second_half(it, dataset.end());
//     auto fut = std::async(std::launch::async, parallel_merge_join_method, std::move(first_half));
//     auto second_half_merged = parallel_merge_join_method(second_half);
//     auto first_hash_merged = fut.get();
//     merged_result = std::move(merge_two_results(first_hash_merged, second_half_merged));
//     return merged_result;
// }
//
// TEST(ParallelMergeJoinTest, BasicParallelMergeJoinTest) {
//     std::vector<std::vector<RowPosition>> dataset = std::move(generate_test_dataset());
//
//     // ################################### method1 ###################################
//     auto start_method1 = std::chrono::high_resolution_clock::now();
//     auto res1 = map_record_method(dataset);
//     auto end_method1 = std::chrono::high_resolution_clock::now();
//     auto duration1 = std::chrono::duration_cast<std::chrono::milliseconds>(end_method1 - start_method1);
//     INFO_LOG("method1 costs %ld ms", duration1.count())
//     std::sort(res1.begin(), res1.end(), order_func);
//     // ################################### method1 ###################################
//
//     // ################################### method2 ###################################
//     auto start_method2 = std::chrono::high_resolution_clock::now();
//     auto res2 = merge_join_method(dataset);
//     auto end_method2 = std::chrono::high_resolution_clock::now();
//     auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end_method2 - start_method2);
//     INFO_LOG("method2 costs %ld ms", duration2.count())
//     std::sort(res2.begin(), res2.end(), order_func);
//     // ################################### method2 ###################################
//
//     // ################################### method3 ###################################
//     auto start_method3 = std::chrono::high_resolution_clock::now();
//     auto res3 = parallel_merge_join_method(dataset);
//     auto end_method3 = std::chrono::high_resolution_clock::now();
//     auto duration3 = std::chrono::duration_cast<std::chrono::milliseconds>(end_method3 - start_method3);
//     INFO_LOG("method3 costs %ld ms", duration3.count())
//     std::sort(res3.begin(), res3.end(), order_func);
//     // ################################### method3 ###################################
//
//     ASSERT_EQ(res1, res2);
//     ASSERT_EQ(res1, res3);
// }
//
// }
//
