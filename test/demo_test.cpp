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

static bool compare_rows(const Row& lhs, const Row& rhs) {
    bool res = (lhs.vin == rhs.vin) && (lhs.timestamp == rhs.timestamp);
    assert(lhs.columns.size() == rhs.columns.size());
    for (const auto& item : lhs.columns) {
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
            res = res && (lhs_val.first == rhs_val.first) && (std::strncmp(lhs_val.second, rhs_val.second, lhs_val.first) == 0);
            break;
        }
        default:
            return false;
        }
    }
    return res;
}

static std::vector<Row> generate_dataset() {
    std::ifstream inputFile(std::filesystem::current_path() / "test_data.csv");
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
        row.vin = Vin(randomString);
        row.timestamp = timestamp;
        row.columns = std::move(columns_map);
        dataset.push_back(std::move(row));
    }

    inputFile.close();
    return dataset;
}

TEST(DemoTest, BasicDemoTest) {
    const size_t N = 100000;
    const std::string TABLE_NAME = "demo";
    io::FileSystemSPtr fs = io::FileSystem::create(std::filesystem::current_path());
    io::Path table_path = std::filesystem::current_path() / io::Path("test_data");
    if (fs->exists(table_path)) {
        fs->delete_directory(table_path);
    }
    fs->create_directory(table_path);
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
    // insert data
    std::vector<Row> src_rows = std::move(generate_dataset());
    size_t index = 0;

    for (size_t i = 0; i < N / 500; ++i) {
        std::vector<Row> batch_rows;
        for (size_t j = 0; j < 500; ++j) {
            batch_rows.emplace_back(src_rows[index++]);
        }
        WriteRequest wq {TABLE_NAME, std::move(batch_rows)};
        demo->upsert(wq);
    }

    ASSERT_EQ(index, N);

    // executeTimeRangeQuery
    LatestQueryRequest lqr;
    lqr.tableName = TABLE_NAME;
    lqr.vins = {Vin("qcklgWrvuPNOzrFny"), Vin("miPhqUBMEruDSsuDE"), Vin("mriegyTJAdRBPojHb")};
    lqr.requestedColumns = {};
    std::vector<Row> lq_res;
    demo->executeLatestQuery(lqr, lq_res);

    std::sort(lq_res.begin(), lq_res.end(), [] (const Row& lhs, const Row& rhs) {
        if (lhs.vin != rhs.vin) {
            return lhs.vin < rhs.vin;
        }
        return lhs.timestamp < rhs.timestamp;
    });

    ASSERT_EQ(lq_res.size(), 3);
    ASSERT_EQ(lq_res[0].timestamp, 1678577116);
    ASSERT_EQ(lq_res[1].timestamp, 1667714866);
    ASSERT_EQ(lq_res[2].timestamp, 1678643945);

    // executeTimeRangeQuery
    TimeRangeQueryRequest trqr;
    trqr.tableName = TABLE_NAME;
    trqr.vin = Vin("gTAhiCUpwRkRkcHkb");
    trqr.timeLowerBound = 47002551;
    trqr.timeUpperBound = 1236032062;
    trqr.requestedColumns = {};
    std::vector<Row> trqr_res;
    std::vector<Row> trqr_ground_truth;
    demo->executeTimeRangeQuery(trqr, trqr_res);

    for (const auto& row : src_rows) {
        if (row.vin == trqr.vin && row.timestamp >= trqr.timeLowerBound && row.timestamp < trqr.timeUpperBound) {
            trqr_ground_truth.push_back(row);
        }
    }

    std::sort(trqr_res.begin(), trqr_res.end(), [] (const Row& lhs, const Row& rhs) {
        if (lhs.vin != rhs.vin) {
            return lhs.vin < rhs.vin;
        }
        return lhs.timestamp < rhs.timestamp;
    });

    std::sort(trqr_ground_truth.begin(), trqr_ground_truth.end(), [] (const Row& lhs, const Row& rhs) {
        if (lhs.vin != rhs.vin) {
            return lhs.vin < rhs.vin;
        }
        return lhs.timestamp < rhs.timestamp;
    });

    ASSERT_EQ(trqr_res.size(), trqr_ground_truth.size());

    for (size_t i = 0; i < trqr_res.size(); ++i) {
        ASSERT_TRUE(compare_rows(trqr_res[i], trqr_ground_truth[i]));
    }

    // shutdown
    ASSERT_EQ(0, demo->shutdown());
}

}

