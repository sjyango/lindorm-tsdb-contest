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
#include <algorithm>
#include <filesystem>
#include <unordered_map>

#include <gtest/gtest.h>

#include "common/slice.h"
#include "storage/indexs/ordinal_key_index.h"
#include "io/file_writer.h"
#include "io/file_reader.h"

namespace LindormContest::test {

using namespace storage;

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

static io::FileWriterPtr generate_file_writer() {
    io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
    io::Path file_path = root_path / io::Path("ordinal_index_test.dat");
    io::FileSystemSPtr fs = io::FileSystem::create(root_path);
    if (fs->exists(file_path)) {
        fs->delete_file(file_path);
    }
    assert(!fs->exists(file_path));
    return fs->create_file(file_path);
}

static io::FileReaderSPtr generate_file_reader() {
    io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
    io::Path file_path = root_path / io::Path("ordinal_index_test.dat");
    io::FileSystemSPtr fs = io::FileSystem::create(root_path);
    assert(fs->exists(file_path));
    return fs->open_file(file_path);
}

TEST(OrdinalKeyIndexTest, BasicOrdinalKeyIndexTest) {
    const int N = 10000;
    io::FileWriterPtr file_writer = generate_file_writer();
    OrdinalIndexWriter writer;
    OrdinalIndexReader reader;
    std::unordered_map<uint64_t, io::PagePointer> maps;
    std::vector<ordinal_t> ordinals;

    for (int i = 0; i < N; ++i) {
        size_t ordinal = i * 1024;
        io::PagePointer pointer(static_cast<uint64_t>(generate_random_int32() % 10000),
                                static_cast<uint32_t>(generate_random_int32() % 10000));
        maps.emplace(ordinal, pointer);
        ordinals.push_back(ordinal);
        writer.append_entry(ordinal, pointer);
    }

    std::shared_ptr<OrdinalIndexMeta> meta = std::make_shared<OrdinalIndexMeta>();
    writer.finish(file_writer.get(), meta);
    file_writer->finalize();
    file_writer->close();
    io::FileReaderSPtr file_reader = generate_file_reader();
    reader.load(file_reader, *meta, N);

    for (auto iter = reader.begin(); iter != reader.end(); ++iter) {
        io::PagePointer lhs = iter.page_pointer();
        io::PagePointer rhs = maps.at(iter.first_ordinal());
        ASSERT_EQ(lhs, rhs);
    }

    for (size_t i = 0; i < N; ++i) {
        ordinal_t o = generate_random_int32() % (N * 1024);
        auto iter = reader.seek_at_or_before(o);
        auto it = std::upper_bound(ordinals.begin(), ordinals.end(), o);
        if (it != ordinals.begin()) {
            --it;
        }
        ASSERT_EQ(iter.page_pointer(), maps.at(*it));
    }
    file_reader->close();
}

}

