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

#include <string>
#include <random>

#include <gtest/gtest.h>

#include "io/file_system.h"
#include "io/file_reader.h"
#include "io/file_writer.h"

namespace LindormContest::test {

inline std::string generate_random_string(int length) {
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

inline int32_t generate_random_int32() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int32_t> dis(0, std::numeric_limits<int32_t>::max());
    return dis(gen);
}

TEST(IOTest, BasicIOTest) {
    io::Path ROOT_PATH = "/home/ysj/lindorm-tsdb-contest-mac/test";
    io::FileSystemSPtr fs = io::FileSystem::create(std::move(ROOT_PATH));
    io::Path dir_name = "test_data";
    io::Path file_name = "test.txt";
    if (!fs->exists(ROOT_PATH / dir_name)) {
        fs->create_directory(ROOT_PATH / dir_name);
    }
    ASSERT_TRUE(fs->exists(ROOT_PATH / dir_name));
    io::FileWriterPtr file_writer;
    if (fs->exists(ROOT_PATH / dir_name / file_name)) {
        fs->delete_file(ROOT_PATH / dir_name / file_name);
    }
    ASSERT_FALSE(fs->exists(ROOT_PATH / dir_name / file_name));
    file_writer = fs->create_file(ROOT_PATH / dir_name / file_name);
    ASSERT_TRUE(fs->exists(ROOT_PATH / dir_name / file_name));
    ASSERT_TRUE(file_writer != nullptr);
    const int N = 1024;
    const int SIZE = N * N;
    std::string src;
    std::string dst;
    src.resize(SIZE);
    dst.resize(SIZE);
    std::vector<Slice> slices;
    size_t offset = 0;

    for (int i = 0; i < N; ++i) {
        std::string s = generate_random_string(N);
        slices.emplace_back(Slice(src.data() + offset, N));
        std::memcpy(src.data() + offset, s.c_str(), N);
        offset += N;
    }

    ASSERT_EQ(offset, SIZE);
    file_writer->appendv(slices.data(), slices.size());
    file_writer->finalize();
    file_writer->close();
    ASSERT_TRUE(fs->exists(ROOT_PATH / dir_name / file_name));
    io::FileReaderSPtr file_reader = fs->open_file(ROOT_PATH / dir_name / file_name);
    offset = 0;
    size_t size_to_read = 0;
    Slice slice(dst.data(), SIZE);
    file_reader->read_at(offset, slice, &size_to_read);
    ASSERT_EQ(size_to_read, SIZE);
    ASSERT_EQ(src, dst);
    offset = 0;

    for (int i = 0; i < N; ++i) {
        size_t new_size_to_read = 0;
        Slice new_slice(dst.data() + offset, N);
        file_reader->read_at(offset, new_slice, &new_size_to_read);
        ASSERT_EQ(new_size_to_read, N);
        ASSERT_EQ(0, std::memcmp(new_slice._data, src.data() + offset, new_slice._size));
        offset += new_size_to_read;
    }
}

}

