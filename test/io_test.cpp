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
        Path tsm_file_path = std::filesystem::current_path() / "demo" / "LSVNV2182E0200009" / "17-0.tsm";
        Footer footer;
        std::string buf;
        io::stream_read_string_from_file(tsm_file_path, 10334, 2888, buf);
        const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.c_str());
        footer.decode_from(p, 360);
    }
}