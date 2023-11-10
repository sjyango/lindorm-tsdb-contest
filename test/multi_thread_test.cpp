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
#include <thread>
#include <unordered_map>
#include <chrono>
#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

#include "Root.h"
#include "TSDBEngineImpl.h"
#include "struct/Requests.h"

namespace LindormContest::test {

    std::vector<Row> global_datasets;
    std::vector<Row> written_datasets;
    std::mutex dataset_mutex;
    std::mutex insert_mutex;

    constexpr int64_t MIN_TS = 1694043124000;
    constexpr int64_t MAX_TS = 1694079124000;

    std::unordered_map<std::string, Row> latest_records; // vin -> max_timestamp
    std::unordered_map<std::string, std::vector<Row>> time_range_records; // vin -> timestamps

    static Vin generate_vin(std::string s) {
        Vin vin;
        std::strncpy(vin.vin, s.c_str(), 17);
        return vin;
    }

    static std::string generate_random_string(int length) {
        const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

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

    static int64_t generate_random_timestamp(int64_t start, int64_t end) {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<int64_t> dis(start, end);
        return dis(gen);
    }

    static double_t generate_random_float64() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<double> dis(0.0, 1.0);
        return dis(gen);
    }

    static Schema generate_schema() {
        std::map<std::string, ColumnType> columnTypeMap;
        columnTypeMap.insert({"col1", COLUMN_TYPE_STRING});
        columnTypeMap.insert({"col2", COLUMN_TYPE_INTEGER});
        columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
        Schema schema;
        schema.columnTypeMap = std::move(columnTypeMap);
        return schema;
    }

    static bool double_equal(double_t lhs, double_t rhs) {
        return std::fabs(lhs - rhs) < EPSILON || (lhs == DOUBLE_NAN && rhs == DOUBLE_NAN);
    }

    static bool compare_rows(const Row &lhs, const Row &rhs) {
        bool res = (lhs.vin == rhs.vin) && (lhs.timestamp == rhs.timestamp);

        for (const auto &item: lhs.columns) {
            switch (item.second.columnType) {
                case COLUMN_TYPE_INTEGER: {
                    int32_t lhs_val;
                    int32_t rhs_val;
                    item.second.getIntegerValue(lhs_val);
                    auto it = rhs.columns.find(item.first);
                    assert(it != rhs.columns.end());
                    it->second.getIntegerValue(rhs_val);
                    res = res && (lhs_val == rhs_val);
                    break;
                }
                case COLUMN_TYPE_DOUBLE_FLOAT: {
                    double_t lhs_val;
                    double_t rhs_val;
                    item.second.getDoubleFloatValue(lhs_val);
                    auto it = rhs.columns.find(item.first);
                    assert(it != rhs.columns.end());
                    it->second.getDoubleFloatValue(rhs_val);
                    res = res && double_equal(lhs_val, rhs_val);
                    break;
                }
                case COLUMN_TYPE_STRING: {
                    std::pair<int32_t, const char *> lhs_val;
                    std::pair<int32_t, const char *> rhs_val;
                    item.second.getStringValue(lhs_val);
                    auto it = rhs.columns.find(item.first);
                    assert(it != rhs.columns.end());
                    it->second.getStringValue(rhs_val);
                    res = res && (lhs_val.first == rhs_val.first) &&
                          (std::strncmp(lhs_val.second, rhs_val.second, lhs_val.first) == 0);
                    break;
                }
                default:
                    return false;
            }
        }
        return res;
    }

    static void generate_dataset() {
        std::string dataset_filename = "10vin_10h.csv";
        std::ifstream inputFile(std::filesystem::current_path() / dataset_filename);
        assert(inputFile.is_open());
        std::vector<Row> dataset;
        std::string line;

        while (std::getline(inputFile, line)) {
            std::vector<std::string> columns;
            std::istringstream ss(line);
            std::string column;

            while (std::getline(ss, column, ',')) {
                columns.push_back(column);
            }

            if (columns.size() != 5) {
                std::cout << "Invalid number of columns." << std::endl;
                continue;
            }

            // 解析数据
            std::string randomString = columns[0];
            int64_t timestamp = std::stoll(columns[1]);
            std::string variableString = columns[2];
            int32_t randomInt = std::stoi(columns[3]);
            double randomDouble = std::stod(columns[4]);

            std::map<std::string, ColumnValue> columns_map;
            ColumnValue col1_val(variableString);
            columns_map.insert({"col1", col1_val});
            ColumnValue col2_val(randomInt);
            columns_map.insert({"col2", col2_val});
            ColumnValue col3_val(randomDouble);
            columns_map.insert({"col3", col3_val});
            Row row;
            row.vin = generate_vin(randomString);
            row.timestamp = timestamp;
            row.columns = std::move(columns_map);
            dataset.push_back(std::move(row));
        }


        inputFile.close();
        {
            std::lock_guard<std::mutex> lock(dataset_mutex);
            global_datasets.insert(global_datasets.end(), dataset.begin(), dataset.end());
            INFO_LOG("insert %zu rows into global_datasets, now global_datasets' size is %zu", dataset.size(),
                     global_datasets.size())
        }
    }

    static void handle_latest_query(TSDBEngineImpl &db, const std::string &TABLE_NAME) {
        const size_t N = 100;

        LatestQueryRequest lqr;
        lqr.tableName = TABLE_NAME;
        lqr.requestedColumns = {"col1", "col2", "col3"};
        std::vector<std::string> request_vins;

        for (int i = 0; i < N; ++i) {
            if (generate_random_float64() < 0.9) {
                size_t rand_index = generate_random_int32() % written_datasets.size();
                lqr.vins.push_back(global_datasets[rand_index].vin);
                request_vins.emplace_back(global_datasets[rand_index].vin.vin, 17);
            } else {
                std::string rand_vin = "LSVNV2182E054" + generate_random_string(4);
                Vin vin;
                std::strncpy(vin.vin, rand_vin.c_str(), 17);
                lqr.vins.push_back(vin);
                request_vins.push_back(rand_vin);
            }
        }

        std::vector<Row> lq_ground_truths;
        std::vector<Row> lq_results;
        db.executeLatestQuery(lqr, lq_results);

        for (const auto &vin: request_vins) {
            if (latest_records.find(vin) != latest_records.end()) {
                lq_ground_truths.push_back(latest_records[vin]);
            }
        }

        std::sort(lq_ground_truths.begin(), lq_ground_truths.end(), [](const Row &lhs, const Row &rhs) {
            if (lhs.vin != rhs.vin) {
                return lhs.vin < rhs.vin;
            }
            return lhs.timestamp < rhs.timestamp;
        });

        std::sort(lq_results.begin(), lq_results.end(), [](const Row &lhs, const Row &rhs) {
            if (lhs.vin != rhs.vin) {
                return lhs.vin < rhs.vin;
            }
            return lhs.timestamp < rhs.timestamp;
        });

        if (lq_results.size() != lq_ground_truths.size()) {
            ASSERT_EQ(lq_results.size(), lq_ground_truths.size());
        }

        for (size_t i = 0; i < lq_results.size(); ++i) {
            if (!compare_rows(lq_results[i], lq_ground_truths[i])) {
                ASSERT_TRUE(compare_rows(lq_results[i], lq_ground_truths[i]));
            }
        }

        // INFO_LOG("[handle_latest_query] finished %lu times, results size is %zu", id + 1, lq_results.size())
    }

    static void handle_time_range_query(TSDBEngineImpl &db, const std::string &TABLE_NAME) {
        TimeRangeQueryRequest trqr;
        trqr.tableName = TABLE_NAME;
        if (generate_random_float64() < 0.9) {
            trqr.vin = global_datasets[generate_random_int32() % written_datasets.size()].vin;
        } else {
            std::string rand_vin = "LSVNV2182E054" + generate_random_string(4);
            std::strncpy(trqr.vin.vin, rand_vin.c_str(), 17);
        }
        trqr.timeLowerBound = generate_random_timestamp(MIN_TS, MAX_TS - 600 * 1000);
        trqr.timeUpperBound = trqr.timeLowerBound + 600 * 1000;
        // trqr.timeUpperBound = generate_random_timestamp(trqr.timeLowerBound, MAX_TS);
        trqr.requestedColumns = {"col1", "col2", "col3"};
        std::string key(trqr.vin.vin, 17);

        std::vector<Row> trq_ground_truths;
        std::vector<Row> trq_results;
        db.executeTimeRangeQuery(trqr, trq_results);

        if (time_range_records.find(key) != time_range_records.end()) {
            for (const auto &row: time_range_records[key]) {
                if (row.timestamp >= trqr.timeLowerBound && row.timestamp < trqr.timeUpperBound) {
                    Row r;
                    std::strncpy(r.vin.vin, row.vin.vin, 17);
                    r.timestamp = row.timestamp;
                    r.columns = row.columns;
                    trq_ground_truths.push_back(std::move(r));
                }
            }
        }

        std::sort(trq_ground_truths.begin(), trq_ground_truths.end(), [](const Row &lhs, const Row &rhs) {
            if (lhs.vin != rhs.vin) {
                return lhs.vin < rhs.vin;
            }
            return lhs.timestamp < rhs.timestamp;
        });

        std::sort(trq_results.begin(), trq_results.end(), [](const Row &lhs, const Row &rhs) {
            if (lhs.vin != rhs.vin) {
                return lhs.vin < rhs.vin;
            }
            return lhs.timestamp < rhs.timestamp;
        });

        if (trq_results.size() != trq_ground_truths.size()) {
            ASSERT_EQ(trq_results.size(), trq_ground_truths.size());
        }

        for (size_t i = 0; i < trq_results.size(); ++i) {
            if (!compare_rows(trq_results[i], trq_ground_truths[i])) {
                ASSERT_TRUE(compare_rows(trq_results[i], trq_ground_truths[i]));
            }
        }

        // INFO_LOG("[handle_time_range_query] finished %lu times, results size is %zu", id + 1, trq_results.size())
    }

    static void handle_aggregate_query(TSDBEngineImpl &db, const std::string &TABLE_NAME) {
        TimeRangeAggregationRequest trar;
        trar.tableName = TABLE_NAME;
        if (generate_random_float64() < 0.9) {
            trar.vin = global_datasets[generate_random_int32() % written_datasets.size()].vin;
        } else {
            std::string rand_vin = "LSVNV2182E054" + generate_random_string(4);
            std::strncpy(trar.vin.vin, rand_vin.c_str(), 17);
        }
        trar.timeLowerBound = generate_random_timestamp(MIN_TS, MAX_TS);
        trar.timeUpperBound = generate_random_timestamp(trar.timeLowerBound, MAX_TS);
        if (generate_random_float64() < 0.5) {
            trar.columnName = "col2";
        } else {
            trar.columnName = "col3";
        }
        if (generate_random_float64() < 0.5) {
            trar.aggregator = MAX;
        } else {
            trar.aggregator = AVG;
        }
        std::string key(trar.vin.vin, VIN_LENGTH);
        Row tra_ground_truth;
        std::vector<Row> tra_results;
        db.executeAggregateQuery(trar, tra_results);

        if (time_range_records.find(key) != time_range_records.end()) {
            if (trar.aggregator == AVG) {
                double_t sum_value = 0.0;
                size_t sum_count = 0;

                for (auto& row: time_range_records[key]) {
                    if (row.timestamp >= trar.timeLowerBound && row.timestamp < trar.timeUpperBound) {
                        if (trar.columnName == "col2") {
                            int32_t int_value;
                            row.columns[trar.columnName].getIntegerValue(int_value);
                            sum_value += int_value;
                        } else if (trar.columnName == "col3") {
                            double_t double_value;
                            row.columns[trar.columnName].getDoubleFloatValue(double_value);
                            sum_value += double_value;
                        }
                        sum_count++;
                    }
                }

                ColumnValue avg_value(sum_value / sum_count);
                tra_ground_truth.vin = trar.vin;
                tra_ground_truth.timestamp = trar.timeLowerBound;
                tra_ground_truth.columns.emplace(trar.columnName, std::move(avg_value));
            } else if (trar.aggregator == MAX) {
                if (trar.columnName == "col2") {
                    int32_t max_value = std::numeric_limits<int32_t>::min();

                    for (auto& row: time_range_records[key]) {
                        if (row.timestamp >= trar.timeLowerBound && row.timestamp < trar.timeUpperBound) {
                            int32_t int_value;
                            row.columns[trar.columnName].getIntegerValue(int_value);
                            max_value = std::max(max_value, int_value);
                        }
                    }

                    ColumnValue max_column_value(max_value);
                    tra_ground_truth.vin = trar.vin;
                    tra_ground_truth.timestamp = trar.timeLowerBound;
                    tra_ground_truth.columns.emplace(trar.columnName, std::move(max_column_value));
                } else if (trar.columnName == "col3") {
                    double_t max_value = std::numeric_limits<double_t>::min();

                    for (auto& row: time_range_records[key]) {
                        if (row.timestamp >= trar.timeLowerBound && row.timestamp < trar.timeUpperBound) {
                            double_t double_value;
                            row.columns[trar.columnName].getDoubleFloatValue(double_value);
                            max_value = std::max(max_value, double_value);
                        }
                    }

                    ColumnValue max_column_value(max_value);
                    tra_ground_truth.vin = trar.vin;
                    tra_ground_truth.timestamp = trar.timeLowerBound;
                    tra_ground_truth.columns.emplace(trar.columnName, std::move(max_column_value));
                }
            }
        }

        if (!tra_results.empty()) {
            if (!compare_rows(tra_results[0], tra_ground_truth)) {
                ASSERT_TRUE(compare_rows(tra_results[0], tra_ground_truth));
            }
        }

        // INFO_LOG("[handle_aggregate_query] finished %lu times, results size is %zu", id + 1, tra_results.size())
    }

    static void handle_downsample_query(TSDBEngineImpl &db, const std::string &TABLE_NAME) {
        TimeRangeDownsampleRequest trdr;
        trdr.tableName = TABLE_NAME;
        if (generate_random_float64() < 0.9) {
            trdr.vin = global_datasets[generate_random_int32() % written_datasets.size()].vin;
        } else {
            std::string rand_vin = "LSVNV2182E054" + generate_random_string(4);
            std::strncpy(trdr.vin.vin, rand_vin.c_str(), 17);
        }
        const size_t INTERVAL_NUM = 100;
        trdr.interval = 100000;
        trdr.timeLowerBound = generate_random_timestamp(MIN_TS, MAX_TS - (INTERVAL_NUM * trdr.interval));
        trdr.timeUpperBound = trdr.timeLowerBound + INTERVAL_NUM * trdr.interval;
        if (generate_random_float64() < 0.5) {
            trdr.columnName = "col2";
            ColumnValue cv(347523745);
            CompareExpression ce;
            ce.value = cv;
            if (generate_random_float64() < 0.5) {
                ce.compareOp = GREATER;
            } else {
                ce.compareOp = EQUAL;
            }
            trdr.columnFilter = ce;
        } else {
            trdr.columnName = "col3";
            ColumnValue cv(100.0);
            CompareExpression ce;
            ce.value = cv;
            if (generate_random_float64() < 0.5) {
                ce.compareOp = GREATER;
            } else {
                ce.compareOp = EQUAL;
            }
            trdr.columnFilter = ce;
        }
        if (generate_random_float64() < 0.5) {
            trdr.aggregator = MAX;
        } else {
            trdr.aggregator = AVG;
        }
        std::string key(trdr.vin.vin, VIN_LENGTH);
        std::vector<Row> trds_ground_truths;
        std::vector<Row> trds_results;
        db.executeDownsampleQuery(trdr, trds_results);

        if (time_range_records.find(key) != time_range_records.end()) {
            for (size_t i = 0; i < INTERVAL_NUM; ++i) {
                int64_t start_time = trdr.timeLowerBound + i * trdr.interval;
                int64_t end_time = start_time + trdr.interval;

                if (trdr.aggregator == AVG) {
                    double_t interval_sum_value = 0.0;
                    size_t interval_sum_count = 0;
                    size_t interval_tr_count = 0;

                    for (auto& row: time_range_records[key]) {
                        if (row.timestamp >= start_time && row.timestamp < end_time) {
                            interval_tr_count++;
                            if (!trdr.columnFilter.doCompare(row.columns[trdr.columnName])) {
                                continue;
                            }
                            if (trdr.columnName == "col2") {
                                int32_t int_value;
                                row.columns[trdr.columnName].getIntegerValue(int_value);
                                interval_sum_value += int_value;
                            } else if (trdr.columnName == "col3") {
                                double_t double_value;
                                row.columns[trdr.columnName].getDoubleFloatValue(double_value);
                                interval_sum_value += double_value;
                            }
                            interval_sum_count++;
                        }
                    }

                    if (interval_tr_count != 0) {
                        if (interval_sum_count == 0) {
                            // filter all data
                            Row result_row;
                            ColumnValue interval_avg_value(DOUBLE_NAN);
                            result_row.vin = trdr.vin;
                            result_row.timestamp = start_time;
                            result_row.columns.emplace(trdr.columnName, std::move(interval_avg_value));
                            trds_ground_truths.emplace_back(std::move(result_row));
                        } else {
                            Row result_row;
                            ColumnValue interval_avg_value(interval_sum_value / interval_sum_count);
                            result_row.vin = trdr.vin;
                            result_row.timestamp = start_time;
                            result_row.columns.emplace(trdr.columnName, std::move(interval_avg_value));
                            trds_ground_truths.emplace_back(std::move(result_row));
                        }
                    }
                } else if (trdr.aggregator == MAX) {
                    int32_t interval_max_int_value = std::numeric_limits<int32_t>::min();
                    double_t interval_max_double_value = std::numeric_limits<double_t>::min();
                    size_t interval_max_count = 0;
                    size_t interval_tr_count = 0;

                    for (auto& row: time_range_records[key]) {
                        if (row.timestamp >= start_time && row.timestamp < end_time) {
                            interval_tr_count++;
                            if (!trdr.columnFilter.doCompare(row.columns[trdr.columnName])) {
                                continue;
                            }
                            if (trdr.columnName == "col2") {
                                int32_t interval_int_value;
                                row.columns[trdr.columnName].getIntegerValue(interval_int_value);
                                interval_max_int_value = std::max(interval_max_int_value, interval_int_value);
                            } else if (trdr.columnName == "col3") {
                                double_t interval_double_value;
                                row.columns[trdr.columnName].getDoubleFloatValue(interval_double_value);
                                interval_max_double_value = std::max(interval_max_double_value, interval_double_value);
                            }
                            interval_max_count++;
                        }
                    }

                    if (interval_tr_count != 0) {
                        if (interval_max_count == 0) {
                            // filter all data
                            if (trdr.columnName == "col2") {
                                Row result_row;
                                ColumnValue interval_max_value(INT_NAN);
                                result_row.vin = trdr.vin;
                                result_row.timestamp = start_time;
                                result_row.columns.emplace(trdr.columnName, std::move(interval_max_value));
                                trds_ground_truths.emplace_back(std::move(result_row));
                            } else if (trdr.columnName == "col3") {
                                Row result_row;
                                ColumnValue interval_max_value(DOUBLE_NAN);
                                result_row.vin = trdr.vin;
                                result_row.timestamp = start_time;
                                result_row.columns.emplace(trdr.columnName, std::move(interval_max_value));
                                trds_ground_truths.emplace_back(std::move(result_row));
                            }
                        } else {
                            if (trdr.columnName == "col2") {
                                Row result_row;
                                ColumnValue interval_max_value(interval_max_int_value);
                                result_row.vin = trdr.vin;
                                result_row.timestamp = start_time;
                                result_row.columns.emplace(trdr.columnName, std::move(interval_max_value));
                                trds_ground_truths.emplace_back(std::move(result_row));
                            } else if (trdr.columnName == "col3") {
                                Row result_row;
                                ColumnValue interval_max_value(interval_max_double_value);
                                result_row.vin = trdr.vin;
                                result_row.timestamp = start_time;
                                result_row.columns.emplace(trdr.columnName, std::move(interval_max_value));
                                trds_ground_truths.emplace_back(std::move(result_row));
                            }
                        }
                    }
                }
            }
        }

        std::sort(trds_ground_truths.begin(), trds_ground_truths.end(), [](const Row &lhs, const Row &rhs) {
            if (lhs.vin != rhs.vin) {
                return lhs.vin < rhs.vin;
            }
            return lhs.timestamp < rhs.timestamp;
        });

        std::sort(trds_results.begin(), trds_results.end(), [](const Row &lhs, const Row &rhs) {
            if (lhs.vin != rhs.vin) {
                return lhs.vin < rhs.vin;
            }
            return lhs.timestamp < rhs.timestamp;
        });

        if (trds_results.size() != trds_ground_truths.size()) {
            ASSERT_EQ(trds_results.size(), trds_ground_truths.size());
        }

        for (size_t i = 0; i < trds_results.size(); ++i) {
            if (!compare_rows(trds_results[i], trds_ground_truths[i])) {
                ASSERT_TRUE(compare_rows(trds_results[i], trds_ground_truths[i]));
            }
        }

        // INFO_LOG("[handle_down_sample_query] finished %lu times, results size is %zu", id + 1, trds_results.size())
    }

    static void handle_multi_query(TSDBEngineImpl &db, const std::string &TABLE_NAME, size_t exec_time) {
        for (int i = 0; i < exec_time; ++i) {
            auto rand_num = generate_random_float64();
            if (rand_num >= 0 && rand_num < 0.25) {
                handle_latest_query(db, TABLE_NAME);
            } else if (rand_num >= 0.25 && rand_num < 0.5) {
                handle_time_range_query(db, TABLE_NAME);
            } else if (rand_num >= 0.5 && rand_num < 0.75) {
                handle_aggregate_query(db, TABLE_NAME);
            } else {
                handle_downsample_query(db, TABLE_NAME);
            }
        }
    }

    static void insert_data_into_db_engine(TSDBEngineImpl &db, const std::string &TABLE_NAME) {
        const size_t INSERT_DATA_THREADS = 16;
        std::thread insert_data_threads[INSERT_DATA_THREADS];

        auto insert_data = [TABLE_NAME](TSDBEngineImpl &db, size_t thread_id) {
            std::lock_guard<std::mutex> l(insert_mutex);
            size_t batch_nums = global_datasets.size() / INSERT_DATA_THREADS;
            size_t start_index = thread_id * batch_nums;
            size_t end_index = std::min((thread_id + 1) * batch_nums, global_datasets.size());
            // insert data
            std::vector<Row> src_rows = std::vector<Row>(global_datasets.begin() + start_index,
                                                         global_datasets.begin() + end_index);
            size_t index = 0;

            for (size_t i = 0; i < src_rows.size() / 500; ++i) {
                std::vector<Row> batch_rows;
                for (size_t j = 0; j < 500; ++j) {
                    batch_rows.emplace_back(src_rows[index++]);
                }
                WriteRequest wq{TABLE_NAME, std::move(batch_rows)};
                db.write(wq);
            }

            if (index < src_rows.size()) {
                WriteRequest wq{TABLE_NAME, std::vector<Row>(src_rows.begin() + index, src_rows.end())};
                db.write(wq);
            }

            written_datasets.insert(written_datasets.end(), src_rows.begin(), src_rows.end());

            for (const auto &row: src_rows) {
                std::string key = std::string(row.vin.vin, 17);
                if (latest_records.find(key) == latest_records.end() || row.timestamp > latest_records[key].timestamp) {
                    std::strncpy(latest_records[key].vin.vin, row.vin.vin, 17);
                    latest_records[key].timestamp = row.timestamp;
                    latest_records[key].columns = row.columns;
                }
                time_range_records[key].push_back(row);
            }

            if (generate_random_float64() < 0.5) {
                handle_multi_query(db, TABLE_NAME, 5);
            }
        };

        for (size_t i = 0; i < INSERT_DATA_THREADS; ++i) {
            insert_data_threads[i] = std::thread(insert_data, std::ref(db), i);
        }

        for (auto &thread: insert_data_threads) {
            thread.join();
        }
    }

    TEST(MultiThreadTest, InsertTest) {
        global_datasets.clear();
        written_datasets.clear();
        latest_records.clear();
        time_range_records.clear();

        const size_t READ_FILE_THREADS = 1;
        std::thread read_file_threads[READ_FILE_THREADS];

        for (size_t i = 0; i < READ_FILE_THREADS; ++i) {
            read_file_threads[i] = std::thread(generate_dataset);
        }

        for (auto &thread: read_file_threads) {
            thread.join();
        }

        // create DBEngine
        const std::string TABLE_NAME = "demo";
        Path table_path = std::filesystem::current_path() / TABLE_NAME;
        if (std::filesystem::exists(table_path)) {
            std::filesystem::remove_all(table_path);
        }
        std::filesystem::create_directory(table_path);
        std::unique_ptr<TSDBEngineImpl> demo = std::make_unique<TSDBEngineImpl>(table_path);
        // connect database
        ASSERT_EQ(0, demo->connect());
        // create schema
        Schema schema = generate_schema();
        // create table
        ASSERT_EQ(0, demo->createTable(TABLE_NAME, schema));
        // write data
        RECORD_TIME_COST(INSERT_DATA_INTO_DB, {
            insert_data_into_db_engine(*demo, TABLE_NAME);
        })
        // shutdown
        ASSERT_EQ(0, demo->shutdown());
        INFO_LOG("####################### [demo->shutdown()] finished #######################")
    }

    TEST(MultiThreadTest, LatestQueryTest) {
        global_datasets.clear();
        written_datasets.clear();
        latest_records.clear();
        time_range_records.clear();

        const size_t READ_FILE_THREADS = 1;
        std::thread read_file_threads[READ_FILE_THREADS];

        for (size_t i = 0; i < READ_FILE_THREADS; ++i) {
            read_file_threads[i] = std::thread(generate_dataset);
        }

        for (auto &thread: read_file_threads) {
            thread.join();
        }

        // create DBEngine
        const std::string TABLE_NAME = "demo";
        Path table_path = std::filesystem::current_path() / TABLE_NAME;
        if (std::filesystem::exists(table_path)) {
            std::filesystem::remove_all(table_path);
        }
        std::filesystem::create_directory(table_path);
        std::unique_ptr<TSDBEngineImpl> demo = std::make_unique<TSDBEngineImpl>(table_path);
        // connect database
        ASSERT_EQ(0, demo->connect());
        // create schema
        Schema schema = generate_schema();
        // create table
        ASSERT_EQ(0, demo->createTable(TABLE_NAME, schema));
        // write data
        RECORD_TIME_COST(INSERT_DATA_INTO_DB, {
            insert_data_into_db_engine(*demo, TABLE_NAME);
            INFO_LOG("####################### [insert_data_into_db_engine] finished #######################")
        })

        // handle latest query
        RECORD_TIME_COST(HANDLE_LATEST_QUERY, {
            const size_t LATEST_QUERY_THREADS = 200;
            std::thread latest_query_threads[LATEST_QUERY_THREADS];

            for (size_t i = 0; i < LATEST_QUERY_THREADS; ++i) {
                latest_query_threads[i] = std::thread(handle_latest_query, std::ref(*demo), TABLE_NAME);
            }

            for (auto &thread: latest_query_threads) {
                thread.join();
            }
        })

        // shutdown
        ASSERT_EQ(0, demo->shutdown());
        INFO_LOG("####################### [demo->shutdown()] finished #######################")

        // reset db
        demo = std::make_unique<TSDBEngineImpl>(table_path);

        // connect database
        ASSERT_EQ(0, demo->connect());
        INFO_LOG("####################### [demo->connect()] finished #######################")

        // handle latest query
        RECORD_TIME_COST(HANDLE_LATEST_QUERY, {
             const size_t LATEST_QUERY_THREADS = 200;
             std::thread latest_query_threads[LATEST_QUERY_THREADS];

             for (size_t i = 0; i < LATEST_QUERY_THREADS; ++i) {
                 latest_query_threads[i] = std::thread(handle_latest_query, std::ref(*demo), TABLE_NAME);
             }

             for (auto &thread: latest_query_threads) {
                 thread.join();
             }
        })

    }

    TEST(MultiThreadTest, TimeRangeQueryTest) {
        global_datasets.clear();
        written_datasets.clear();
        latest_records.clear();
        time_range_records.clear();

        const size_t READ_FILE_THREADS = 1;
        std::thread read_file_threads[READ_FILE_THREADS];

        for (size_t i = 0; i < READ_FILE_THREADS; ++i) {
            read_file_threads[i] = std::thread(generate_dataset);
        }

        for (auto &thread: read_file_threads) {
            thread.join();
        }

        // create DBEngine
        const std::string TABLE_NAME = "demo";
        Path table_path = std::filesystem::current_path() / TABLE_NAME;
        if (std::filesystem::exists(table_path)) {
            std::filesystem::remove_all(table_path);
        }
        std::filesystem::create_directory(table_path);
        std::unique_ptr<TSDBEngineImpl> demo = std::make_unique<TSDBEngineImpl>(table_path);
        // connect database
        ASSERT_EQ(0, demo->connect());
        // create schema
        Schema schema = generate_schema();
        // create table
        ASSERT_EQ(0, demo->createTable(TABLE_NAME, schema));
        // write data
        RECORD_TIME_COST(INSERT_DATA_INTO_DB, {
            insert_data_into_db_engine(*demo, TABLE_NAME);
            INFO_LOG("####################### [insert_data_into_db_engine] finished #######################")
        });

        // handle time range query
        RECORD_TIME_COST(HANDLE_TIME_RANGE_QUERY, {
            const size_t TIME_RANGE_QUERY_THREADS = 10;
            std::thread time_range_query_threads[TIME_RANGE_QUERY_THREADS];

            for (size_t i = 0; i < TIME_RANGE_QUERY_THREADS; ++i) {
                time_range_query_threads[i] = std::thread(handle_time_range_query, std::ref(*demo), TABLE_NAME);
            }

            for (auto &thread: time_range_query_threads) {
                thread.join();
            }
        })

        INFO_LOG("####################### finished [handle_time_range_query] #######################")

        // shutdown
        ASSERT_EQ(0, demo->shutdown());
        INFO_LOG("####################### [demo->shutdown()] finished #######################")

        // reset db
        demo = std::make_unique<TSDBEngineImpl>(table_path);

        // connect database
        ASSERT_EQ(0, demo->connect());
        INFO_LOG("####################### [demo->connect()] finished #######################")

        // handle time range query
        RECORD_TIME_COST(HANDLE_TIME_RANGE_QUERY, {
            const size_t TIME_RANGE_QUERY_THREADS = 200;
            std::thread time_range_query_threads[TIME_RANGE_QUERY_THREADS];

            for (size_t i = 0; i < TIME_RANGE_QUERY_THREADS; ++i) {
                time_range_query_threads[i] = std::thread(handle_time_range_query, std::ref(*demo), TABLE_NAME);
            }

            for (auto &thread: time_range_query_threads) {
                thread.join();
            }
        })
    }

    TEST(MultiThreadTest, AggregateQueryTest) {
        global_datasets.clear();
        written_datasets.clear();
        latest_records.clear();
        time_range_records.clear();

        const size_t READ_FILE_THREADS = 1;
        std::thread read_file_threads[READ_FILE_THREADS];

        for (size_t i = 0; i < READ_FILE_THREADS; ++i) {
            read_file_threads[i] = std::thread(generate_dataset);
        }

        for (auto &thread: read_file_threads) {
            thread.join();
        }

        // create DBEngine
        const std::string TABLE_NAME = "demo";
        Path table_path = std::filesystem::current_path() / TABLE_NAME;
        if (std::filesystem::exists(table_path)) {
            std::filesystem::remove_all(table_path);
        }
        std::filesystem::create_directory(table_path);
        std::unique_ptr<TSDBEngineImpl> demo = std::make_unique<TSDBEngineImpl>(table_path);
        // connect database
        ASSERT_EQ(0, demo->connect());
        // create schema
        Schema schema = generate_schema();
        // create table
        ASSERT_EQ(0, demo->createTable(TABLE_NAME, schema));
        // write data
        RECORD_TIME_COST(INSERT_DATA_INTO_DB, {
            insert_data_into_db_engine(*demo, TABLE_NAME);
            INFO_LOG("####################### [insert_data_into_db_engine] finished #######################")
        });

        // handle aggregate query
        RECORD_TIME_COST(HANDLE_AGGREGATE_QUERY, {
            const size_t AGGREGATE_QUERY_THREADS = 10;
            std::thread aggregate_query_threads[AGGREGATE_QUERY_THREADS];

            for (size_t i = 0; i < AGGREGATE_QUERY_THREADS; ++i) {
                aggregate_query_threads[i] = std::thread(handle_aggregate_query, std::ref(*demo), TABLE_NAME);
            }

            for (auto &thread: aggregate_query_threads) {
                thread.join();
            }
        })

        INFO_LOG("####################### finished [handle_aggregate_query] #######################")

        // shutdown
        ASSERT_EQ(0, demo->shutdown());
        INFO_LOG("####################### [demo->shutdown()] finished #######################")

        // reset db
        demo = std::make_unique<TSDBEngineImpl>(table_path);

        // connect database
        ASSERT_EQ(0, demo->connect());
        INFO_LOG("####################### [demo->connect()] finished #######################")

        // handle aggregate query
        RECORD_TIME_COST(HANDLE_AGGREGATE_QUERY, {
            const size_t AGGREGATE_QUERY_THREADS = 200;
            std::thread aggregate_query_threads[AGGREGATE_QUERY_THREADS];

            for (size_t i = 0; i < AGGREGATE_QUERY_THREADS; ++i) {
                aggregate_query_threads[i] = std::thread(handle_aggregate_query, std::ref(*demo), TABLE_NAME);
            }

            for (auto &thread: aggregate_query_threads) {
                thread.join();
            }
        })
    }

    TEST(MultiThreadTest, DownSampleQueryTest) {
        global_datasets.clear();
        written_datasets.clear();
        latest_records.clear();
        time_range_records.clear();

        const size_t READ_FILE_THREADS = 1;
        std::thread read_file_threads[READ_FILE_THREADS];

        for (size_t i = 0; i < READ_FILE_THREADS; ++i) {
            read_file_threads[i] = std::thread(generate_dataset);
        }

        for (auto &thread: read_file_threads) {
            thread.join();
        }

        // create DBEngine
        const std::string TABLE_NAME = "demo";
        Path table_path = std::filesystem::current_path() / TABLE_NAME;
        if (std::filesystem::exists(table_path)) {
            std::filesystem::remove_all(table_path);
        }
        std::filesystem::create_directory(table_path);
        std::unique_ptr<TSDBEngineImpl> demo = std::make_unique<TSDBEngineImpl>(table_path);
        // connect database
        ASSERT_EQ(0, demo->connect());
        // create schema
        Schema schema = generate_schema();
        // create table
        ASSERT_EQ(0, demo->createTable(TABLE_NAME, schema));
        // write data
        RECORD_TIME_COST(INSERT_DATA_INTO_DB, {
            insert_data_into_db_engine(*demo, TABLE_NAME);
            INFO_LOG("####################### [insert_data_into_db_engine] finished #######################")
        })

        // handle downsample query
        RECORD_TIME_COST(HANDLE_DOWNSAMPLE_QUERY, {
            const size_t DOWNSAMPLE_QUERY_THREADS = 10;
            std::thread downsample_query_threads[DOWNSAMPLE_QUERY_THREADS];

            for (size_t i = 0; i < DOWNSAMPLE_QUERY_THREADS; ++i) {
                downsample_query_threads[i] = std::thread(handle_downsample_query, std::ref(*demo), TABLE_NAME);
            }

            for (auto &thread: downsample_query_threads) {
                thread.join();
            }
        })

        INFO_LOG("####################### finished [handle_aggregate_query] #######################")

        // shutdown
        ASSERT_EQ(0, demo->shutdown());
        INFO_LOG("####################### [demo->shutdown()] finished #######################")

        // reset db
        demo = std::make_unique<TSDBEngineImpl>(table_path);

        // connect database
        ASSERT_EQ(0, demo->connect());
        INFO_LOG("####################### [demo->connect()] finished #######################")

        // handle downsample query
        RECORD_TIME_COST(HANDLE_DOWNSAMPLE_QUERY, {
            const size_t DOWNSAMPLE_QUERY_THREADS = 200;
            std::thread downsample_query_threads[DOWNSAMPLE_QUERY_THREADS];

            for (size_t i = 0; i < DOWNSAMPLE_QUERY_THREADS; ++i) {
                downsample_query_threads[i] = std::thread(handle_downsample_query, std::ref(*demo), TABLE_NAME);
            }

            for (auto &thread: downsample_query_threads) {
                thread.join();
            }
        })
    }

    TEST(MultiThreadTest, MultiQueryTest) {
        global_datasets.clear();
        written_datasets.clear();
        latest_records.clear();
        time_range_records.clear();

        const size_t READ_FILE_THREADS = 1;
        std::thread read_file_threads[READ_FILE_THREADS];

        for (size_t i = 0; i < READ_FILE_THREADS; ++i) {
            read_file_threads[i] = std::thread(generate_dataset);
        }

        for (auto &thread: read_file_threads) {
            thread.join();
        }

        // create DBEngine
        const std::string TABLE_NAME = "demo";
        Path table_path = std::filesystem::current_path() / TABLE_NAME;
        if (std::filesystem::exists(table_path)) {
            std::filesystem::remove_all(table_path);
        }
        std::filesystem::create_directory(table_path);
        std::unique_ptr<TSDBEngineImpl> demo = std::make_unique<TSDBEngineImpl>(table_path);
        // connect database
        ASSERT_EQ(0, demo->connect());
        // create schema
        Schema schema = generate_schema();
        // create table
        ASSERT_EQ(0, demo->createTable(TABLE_NAME, schema));
        // write data
        RECORD_TIME_COST(INSERT_DATA_INTO_DB, {
            insert_data_into_db_engine(*demo, TABLE_NAME);
            INFO_LOG("####################### [insert_data_into_db_engine] finished #######################")
        });

        // handle multi query
        RECORD_TIME_COST(HANDLE_MULTI_QUERY, {
            const size_t MULTI_QUERY_THREADS = 20;
            std::thread multi_query_threads[MULTI_QUERY_THREADS];

            for (size_t i = 0; i < MULTI_QUERY_THREADS; ++i) {
                multi_query_threads[i] = std::thread(handle_multi_query, std::ref(*demo), TABLE_NAME, 2);
            }

            for (auto &thread: multi_query_threads) {
                thread.join();
            }
        })

        // shutdown
        ASSERT_EQ(0, demo->shutdown());
        INFO_LOG("####################### [demo->shutdown()] finished #######################")

        // reset db
        demo = std::make_unique<TSDBEngineImpl>(table_path);

        // connect database
        ASSERT_EQ(0, demo->connect());
        INFO_LOG("####################### [demo->connect()] finished #######################")

        // handle multi query
        RECORD_TIME_COST(HANDLE_MULTI_QUERY, {
            const size_t MULTI_QUERY_THREADS = 200;
            std::thread multi_query_threads[MULTI_QUERY_THREADS];

            for (size_t i = 0; i < MULTI_QUERY_THREADS; ++i) {
                multi_query_threads[i] = std::thread(handle_multi_query, std::ref(*demo), TABLE_NAME, 10);
            }

            for (auto &thread: multi_query_threads) {
                thread.join();
            }
        })
    }

}

