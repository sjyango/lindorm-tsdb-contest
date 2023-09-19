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

#include "Root.h"
#include "storage/tsm_file.h"
#include "storage/tsm_writer.h"
#include "struct/Schema.h"
#include "struct/Row.h"

namespace LindormContest::test {

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
        ColumnValue col0_val(generate_random_string(20));
        columns.insert({"col0", col0_val});
        ColumnValue col1_val(generate_random_int32());
        columns.insert({"col1", col1_val});
        ColumnValue col2_val(generate_random_float64());
        columns.insert({"col2", col2_val});

        Row row;
        std::string vin_str = generate_random_string(VIN_LENGTH);
        std::memcpy(row.vin.vin, vin_str.c_str(), VIN_LENGTH);
        row.timestamp = generate_random_timestamp();
        row.columns = std::move(columns);
        return row;
    }

    TEST(TsmTest, BasicTsmTest) {
        const size_t N = 10;
        SchemaSPtr schema = std::make_shared<Schema>();
        schema->columnTypeMap.insert({"col0", COLUMN_TYPE_STRING});
        schema->columnTypeMap.insert({"col1", COLUMN_TYPE_INTEGER});
        schema->columnTypeMap.insert({"col2", COLUMN_TYPE_DOUBLE_FLOAT});

        ThreadPoolSPtr flush_pool = std::make_shared<ThreadPool>(4);
        auto index_manager = std::make_shared<GlobalIndexManager>();
        Path flush_dir_path = std::filesystem::current_path() / "tsm";
        TsmWriter tsm_writer(0, index_manager, flush_pool, flush_dir_path);

        for (size_t i = 0; i < N * MEMMAP_FLUSH_SIZE; ++i) {
            tsm_writer.append(generate_row());
        }

        flush_pool->shutdown();
        std::vector<TsmFile> tsm_files;

        for (const auto& entry: std::filesystem::directory_iterator(flush_dir_path)) {
            TsmFile tsm_file;
            tsm_file.read_from_file(entry.path());
            tsm_files.emplace_back(std::move(tsm_file));
        }

        ASSERT_EQ(tsm_files.size(), N);
    }

    // TEST(TsmTest, TsmCompactionTest) {
    //     const size_t N = 10;
    //     SchemaSPtr schema = std::make_shared<Schema>();
    //     schema->columnTypeMap.insert({"col0", COLUMN_TYPE_STRING});
    //     schema->columnTypeMap.insert({"col1", COLUMN_TYPE_INTEGER});
    //     schema->columnTypeMap.insert({"col2", COLUMN_TYPE_DOUBLE_FLOAT});
    //
    //     ThreadPoolSPtr flush_pool = std::make_shared<ThreadPool>(4);
    //     auto index_manager = std::make_shared<GlobalIndexManager>();
    //     Path flush_dir_path = std::filesystem::current_path() / "tsm";
    //     TsmWriter tsm_writer("", index_manager, flush_pool, flush_dir_path, schema);
    //
    //     for (size_t i = 0; i < N * MEMMAP_FLUSH_SIZE; ++i) {
    //         tsm_writer.append(generate_row());
    //     }
    //
    //     flush_pool->shutdown();
    //     std::vector<TsmFile> tsm_files;
    //
    //     for (const auto& entry: std::filesystem::directory_iterator(flush_dir_path)) {
    //         TsmFile tsm_file;
    //         tsm_file.read_from_file(entry.path());
    //         tsm_files.emplace_back(std::move(tsm_file));
    //     }
    //
    //     ASSERT_EQ(tsm_files.size(), N);
    //     size_t ts_count = 0;
    //     size_t value_count = 0;
    //
    //     for (const auto &tsm_file: tsm_files) {
    //         ts_count += tsm_file._footer._tss.size();
    //
    //         for (const auto &data_block: tsm_file._data_blocks) {
    //             value_count += data_block._column_values.size();
    //         }
    //     }
    //
    //     TsmFile output_file;
    //     multiway_compaction(schema, tsm_files, output_file);
    //     size_t output_value_count = 0;
    //
    //     for (const auto &data_block: output_file._data_blocks) {
    //         output_value_count += data_block._column_values.size();
    //     }
    //
    //     ASSERT_EQ(output_file._footer._tss.size(), ts_count);
    //     ASSERT_EQ(output_value_count, value_count);
    // }

}