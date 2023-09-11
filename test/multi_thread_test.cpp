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

namespace LindormContest::test {

    std::vector<Row> global_datasets;
    std::vector<Row> written_datasets;
    std::mutex dataset_mutex;
    std::mutex insert_mutex;

    std::unordered_map<std::string, Row> latest_records; // vin -> max_timestamp
    std::unordered_map<std::string, std::vector<Row>> time_range_records; // vin -> timestamps

    static Vin generate_vin(std::string s) {
        Vin vin;
        std::strncpy(vin.vin, s.c_str(), 17);
        return vin;
    }

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

    static Schema generate_schema() {
        std::map<std::string, ColumnType> columnTypeMap;
        columnTypeMap.insert({"col1", COLUMN_TYPE_STRING});
        columnTypeMap.insert({"col2", COLUMN_TYPE_INTEGER});
        columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
        Schema schema;
        schema.columnTypeMap = std::move(columnTypeMap);
        return schema;
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
                    res = res && (lhs_val == rhs_val);
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

    static void generate_dataset(size_t dataset_id) {
        // std::string dataset_filename = "test_data_" + std::to_string(dataset_id) + ".csv";
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

    static void generate_index_dataset() {
        for (const auto &row: global_datasets) {
            std::string key = std::string(row.vin.vin, 17);
            if (latest_records.find(key) == latest_records.end() || row.timestamp > latest_records[key].timestamp) {
                std::strncpy(latest_records[key].vin.vin, row.vin.vin, 17);
                latest_records[key].timestamp = row.timestamp;
                latest_records[key].columns = row.columns;
            }
            time_range_records[key].push_back(row);
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
                std::string rand_vin = "LSVNV2182E020" + generate_random_string(4);
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

    static void handle_time_range_query(TSDBEngineImpl &db, const std::string &TABLE_NAME, size_t id) {
        TimeRangeQueryRequest trqr;
        trqr.tableName = TABLE_NAME;
        if (generate_random_float64() < 1) {
            trqr.vin = global_datasets[generate_random_int32() % written_datasets.size()].vin;
        } else {
            std::string rand_vin = "LSVNV2182E020" + generate_random_string(4);
            std::strncpy(trqr.vin.vin, rand_vin.c_str(), 17);
        }
        trqr.timeLowerBound = 1689091225000;
        trqr.timeUpperBound = 1689091275000;
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

//        if (generate_random_float64() < 0.1) {
//            if (generate_random_float64() < 0.5) {
//                handle_latest_query(db, TABLE_NAME, 0);
//                INFO_LOG("###################### execute [handle_latest_query] read after write success ######################")
//            } else {
//                handle_time_range_query(db, TABLE_NAME, 0);
//                INFO_LOG("###################### execute [handle_time_range_query] read after write success ######################")
//            }
//        }
        };

        for (size_t i = 0; i < INSERT_DATA_THREADS; ++i) {
            insert_data_threads[i] = std::thread(insert_data, std::ref(db), i);
        }

        for (auto &thread: insert_data_threads) {
            thread.join();
        }
    }

    TEST(MultiThreadTest, DISABLED_MultiThreadDemoTest) {
        global_datasets.clear();
        written_datasets.clear();
        latest_records.clear();
        time_range_records.clear();

        const size_t READ_FILE_THREADS = 1;
        std::thread read_file_threads[READ_FILE_THREADS];

        // prepare global datasets

        for (size_t i = 0; i < READ_FILE_THREADS; ++i) {
            read_file_threads[i] = std::thread(generate_dataset, i);
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
        std::map<std::string, ColumnType> columnTypeMap;
        columnTypeMap.insert({"col1", COLUMN_TYPE_STRING});
        columnTypeMap.insert({"col2", COLUMN_TYPE_INTEGER});
        columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
        Schema schema;
        schema.columnTypeMap = std::move(columnTypeMap);
        // create table
        ASSERT_EQ(0, demo->createTable(TABLE_NAME, schema));

        // generate_index_dataset();
        // INFO_LOG("####################### [generate_index_dataset] finished #######################")

        // insert_data
        auto start_time = std::chrono::high_resolution_clock::now();
        insert_data_into_db_engine(*demo, TABLE_NAME);
        auto end_time = std::chrono::high_resolution_clock::now();

        INFO_LOG(
                "####################### [insert_data_into_db_engine] finished, cost time: %lds #######################",
                std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count())

        const size_t N = 200;

        // handle latest query
        const size_t LATEST_QUERY_THREADS = N;
        std::thread latest_query_threads[LATEST_QUERY_THREADS];

        for (size_t i = 0; i < LATEST_QUERY_THREADS; ++i) {
            latest_query_threads[i] = std::thread(handle_latest_query, std::ref(*demo), TABLE_NAME);
        }

        for (auto &thread: latest_query_threads) {
            thread.join();
        }

        INFO_LOG("####################### finished [handle_latest_query] #######################")

        // handle time range query
        const size_t TIME_RANGE_QUERY_THREADS = N;
        std::thread time_range_query_threads[TIME_RANGE_QUERY_THREADS];

        for (size_t i = 0; i < TIME_RANGE_QUERY_THREADS; ++i) {
            time_range_query_threads[i] = std::thread(handle_time_range_query, std::ref(*demo), TABLE_NAME, i + 1);
        }

        for (auto &thread: time_range_query_threads) {
            thread.join();
        }

        INFO_LOG("####################### finished [handle_time_range_query] #######################")

        // shutdown
        ASSERT_EQ(0, demo->shutdown());
        INFO_LOG("####################### [demo->shutdown()] finished #######################")

        // reset db
        demo = std::make_unique<TSDBEngineImpl>(table_path);
        // connect database
        ASSERT_EQ(0, demo->connect());
        INFO_LOG("####################### [demo->connect()] finished #######################")

        auto latest_start_time = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < LATEST_QUERY_THREADS; ++i) {
            latest_query_threads[i] = std::thread(handle_latest_query, std::ref(*demo), TABLE_NAME);
        }

        for (auto &thread: latest_query_threads) {
            thread.join();
        }

        auto latest_end_time = std::chrono::high_resolution_clock::now();

        INFO_LOG(
                "####################### finished after reset [handle_latest_query], cost time: %ldms #######################",
                std::chrono::duration_cast<std::chrono::milliseconds>(latest_end_time - latest_start_time).count())

        auto range_start_time = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < TIME_RANGE_QUERY_THREADS; ++i) {
            time_range_query_threads[i] = std::thread(handle_time_range_query, std::ref(*demo), TABLE_NAME, i + 1);
        }

        for (auto &thread: time_range_query_threads) {
            thread.join();
        }

        auto range_end_time = std::chrono::high_resolution_clock::now();

        INFO_LOG(
                "####################### finished after reset [handle_time_range_query], cost time: %ldms #######################",
                std::chrono::duration_cast<std::chrono::milliseconds>(range_end_time - range_start_time).count())

        // shutdown
        ASSERT_EQ(0, demo->shutdown());
    }

    TEST(MultiThreadTest, DISABLED_MultiThreadRandomQueryDemoTest) {
        global_datasets.clear();
        written_datasets.clear();
        latest_records.clear();
        time_range_records.clear();

        const size_t READ_FILE_THREADS = 10;
        std::thread read_file_threads[READ_FILE_THREADS];

        // prepare global datasets

        for (size_t i = 0; i < READ_FILE_THREADS; ++i) {
            read_file_threads[i] = std::thread(generate_dataset, i);
        }

        for (auto &thread: read_file_threads) {
            thread.join();
        }

        // create DBEngine
        const std::string TABLE_NAME = "demo";
        Path table_path = std::filesystem::current_path() / TABLE_NAME;
        std::filesystem::create_directory(table_path);
        std::unique_ptr<TSDBEngineImpl> demo = std::make_unique<TSDBEngineImpl>(table_path);
        // connect database
        ASSERT_EQ(0, demo->connect());
        // create schema
        std::map<std::string, ColumnType> columnTypeMap;
        columnTypeMap.insert({"col1", COLUMN_TYPE_STRING});
        columnTypeMap.insert({"col2", COLUMN_TYPE_INTEGER});
        columnTypeMap.insert({"col3", COLUMN_TYPE_DOUBLE_FLOAT});
        Schema schema;
        schema.columnTypeMap = std::move(columnTypeMap);
        // create table
        ASSERT_EQ(0, demo->createTable(TABLE_NAME, schema));

        // generate_index_dataset();
        // INFO_LOG("####################### [generate_index_dataset] finished #######################")

        // insert_data
        insert_data_into_db_engine(*demo, TABLE_NAME);
        INFO_LOG("####################### [insert_data_into_db_engine] finished #######################")

        const size_t N = 200;

        // handle query
        const size_t QUERY_THREADS = N;
        std::thread query_threads[QUERY_THREADS];

        for (size_t i = 0; i < QUERY_THREADS; ++i) {
            query_threads[i] = std::thread([&demo, TABLE_NAME, i]() {
                if (generate_random_float64() < 0.5) {
                    handle_latest_query(*demo, TABLE_NAME);
                } else {
                    handle_time_range_query(*demo, TABLE_NAME, i + 1);
                }
            });
        }

        for (auto &thread: query_threads) {
            thread.join();
        }

        // shutdown
        ASSERT_EQ(0, demo->shutdown());
        INFO_LOG("####################### [demo->shutdown()] finished #######################")

        // reset db
        demo = std::make_unique<TSDBEngineImpl>(table_path);
        // connect database
        ASSERT_EQ(0, demo->connect());
        INFO_LOG("####################### [demo->connect()] finished #######################")

        for (size_t i = 0; i < QUERY_THREADS; ++i) {
            query_threads[i] = std::thread([&demo, TABLE_NAME, i]() {
                if (generate_random_float64() < 0.5) {
                    handle_latest_query(*demo, TABLE_NAME);
                } else {
                    handle_time_range_query(*demo, TABLE_NAME, i + 1);
                }
            });
        }

        for (auto &thread: query_threads) {
            thread.join();
        }

        // shutdown
        ASSERT_EQ(0, demo->shutdown());
    }

    TEST(MultiThreadTest, DISABLED_MultiThreadInsertTest) {
        global_datasets.clear();
        written_datasets.clear();
        latest_records.clear();
        time_range_records.clear();

        const size_t READ_FILE_THREADS = 1;
        std::thread read_file_threads[READ_FILE_THREADS];

        for (size_t i = 0; i < READ_FILE_THREADS; ++i) {
            read_file_threads[i] = std::thread(generate_dataset, i);
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
        // shutdown
        ASSERT_EQ(0, demo->shutdown());
        INFO_LOG("####################### [demo->shutdown()] finished #######################")

        for (const auto & vin_path : std::filesystem::directory_iterator(table_path)) {
            if (!vin_path.is_directory()) {
                continue;
            }
            size_t file_count = 0;
            for (const auto & tsm_path: std::filesystem::directory_iterator(vin_path.path())) {
                file_count++;
            }
            ASSERT_EQ(file_count, 100);
        }
    }

    TEST(MultiThreadTest, MultiThreadInsertAndLatestTest) {
        global_datasets.clear();
        written_datasets.clear();
        latest_records.clear();
        time_range_records.clear();

        const size_t READ_FILE_THREADS = 1;
        std::thread read_file_threads[READ_FILE_THREADS];

        for (size_t i = 0; i < READ_FILE_THREADS; ++i) {
            read_file_threads[i] = std::thread(generate_dataset, i);
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
             INFO_LOG("####################### [handle latest query] finished #######################")
        });

    }

}

