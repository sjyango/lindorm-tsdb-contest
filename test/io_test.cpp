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

#include <gtest/gtest.h>
#include <random>

#include "io/io_utils.h"
#include "storage/tsm_file.h"

namespace LindormContest::test {

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

    static double_t generate_random_float64() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<double> dis(0.0, 1.0);
        return dis(gen);
    }

    static SchemaSPtr generate_schema() {
        SchemaSPtr schema = std::make_shared<Schema>();
        schema->columnTypeMap.insert({"col1", COLUMN_TYPE_INTEGER});
        schema->columnTypeMap.insert({"col2", COLUMN_TYPE_DOUBLE_FLOAT});
        schema->columnTypeMap.insert({"col3", COLUMN_TYPE_STRING});
        return schema;
    }

    static Row generate_row() {
        Row row;
        std::string vin_str = generate_random_string(VIN_LENGTH);
        std::memcpy(row.vin.vin, vin_str.c_str(), VIN_LENGTH);
        row.timestamp = generate_random_timestamp();
        row.columns.emplace("col1", generate_random_int32());
        row.columns.emplace("col2", generate_random_float64());
        row.columns.emplace("col3", generate_random_string(20));
        return row;
    }

    static bool double_equal(double_t lhs, double_t rhs) {
        return std::fabs(lhs - rhs) < EPSILON || (lhs == DOUBLE_NAN && rhs == DOUBLE_NAN);
    }

    static bool compare_rows(const Row &lhs, const Row &rhs, bool vin_include) {
        bool res = vin_include ? (lhs.vin == rhs.vin) && (lhs.timestamp == rhs.timestamp) : lhs.timestamp == rhs.timestamp;

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

    TEST(IOTest, BasicIOTest) {
        std::string file_content = generate_random_string(20 * 1024 * 1024);
        Path file_path1 = std::filesystem::current_path() / "io_test1.tsm";
        std::string read_content1;
        // RECORD_TIME_COST(IO_TEST1, {
        //      io::mmap_write_string_to_file(file_path1, file_content);
        //      io::mmap_read_string_from_file(file_path1, read_content1);
        // });
        ASSERT_EQ(file_content, read_content1);

        Path file_path2 = std::filesystem::current_path() / "io_test2.tsm";
        std::string read_content2;
        RECORD_TIME_COST(IO_TEST2, {
             io::stream_write_string_to_file(file_path2, file_content);
             io::stream_read_string_from_file(file_path2, read_content2);
        });
        ASSERT_EQ(file_content, read_content2);
    }

    TEST(IOTest, TsmIOTest) {
        const int N = 10;
        Path file_path = std::filesystem::current_path() / "row.dat";
        std::ofstream output_file;
        output_file.open(file_path, std::ios::out | std::ios::binary);
        std::vector<Row> rows;
        SchemaSPtr schema = generate_schema();

        for (int i = 0; i < N; ++i) {
            rows.emplace_back(std::move(generate_row()));
        }

        for (const auto &row: rows) {
            io::write_row_to_file(output_file, schema, row, false);
        }

        output_file.flush();
        output_file.close();
        std::ifstream input_file;
        input_file.open(file_path, std::ios::in | std::ios::binary);
        std::vector<Row> result_rows;

        for (int i = 0; i < N; ++i) {
            Row row;
            io::read_row_from_file(input_file, schema, false, row);
            result_rows.emplace_back(std::move(row));
        }

        ASSERT_EQ(rows.size(), result_rows.size());

        for (int i = 0; i < N; ++i) {
            if (!compare_rows(rows[i], result_rows[i], false)) {
                ASSERT_TRUE(compare_rows(rows[i], result_rows[i], false));
            }
        }
    }
}