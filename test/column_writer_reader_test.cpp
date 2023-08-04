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

#include <filesystem>
#include <random>
#include <cmath>

#include "io/file_system.h"
#include "storage/segment_traits.h"
#include "vec/columns/ColumnFactory.h"
#include "storage/column_writer.h"
#include "storage/column_reader.h"

namespace LindormContest::test {

using namespace storage;
using namespace vectorized;

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

inline int64_t generate_random_timestamp() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, std::numeric_limits<int64_t>::max());
    return dis(gen);
}

inline double_t generate_random_float64() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> dis(0.0, 1.0);
    return dis(gen);
}

static io::FileWriterPtr generate_file_writer() {
    io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
    io::Path file_path = root_path / io::Path("column_writer_reader_test.dat");
    io::FileSystemSPtr fs = io::FileSystem::create(root_path);
    if (fs->exists(file_path)) {
        fs->delete_file(file_path);
    }
    assert(!fs->exists(file_path));
    return fs->create_file(file_path);
}

static io::FileReaderSPtr generate_file_reader() {
    io::Path root_path = std::filesystem::current_path() / io::Path("test_data");
    io::Path file_path = root_path / io::Path("column_writer_reader_test.dat");
    io::FileSystemSPtr fs = io::FileSystem::create(root_path);
    assert(fs->exists(file_path));
    return fs->open_file(file_path);
}

TEST(ColumnWriterReaderTest, Int32ColumnWriterReaderTest) {
    const size_t N = 1000000;
    // ######################################## ColumnWriter ########################################

    io::FileWriterPtr file_writer = generate_file_writer();
    ColumnMetaSPtr meta = std::make_shared<ColumnMeta>();
    meta->_column_id = 0;
    meta->_type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_INTEGER);
    meta->_encoding_type = EncodingType::PLAIN_ENCODING;
    meta->_compression_type = CompressionType::NO_COMPRESSION;
    std::unique_ptr<ColumnWriter> column_writer = std::make_unique<ColumnWriter>(meta, file_writer.get());
    MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_INTEGER, "src");
    std::vector<int32_t> src_nums;

    for (size_t i = 0; i < N; ++i) {
        src_nums.push_back(generate_random_int32());
    }

    const uint8_t* src_ptr = reinterpret_cast<const uint8_t*>(src_nums.data());
    column_writer->append_data(&src_ptr, N);
    column_writer->write_column_data();
    column_writer->write_column_index();
    file_writer->finalize();
    file_writer->close();

    // ######################################## ColumnWriter ########################################

    // ######################################## ColumnReader ########################################

    io::FileReaderSPtr file_reader = generate_file_reader();
    std::unique_ptr<ColumnReader> column_reader = std::make_unique<ColumnReader>(meta, N, file_reader);
    column_reader->seek_to_first();
    size_t read_count = N;
    column_reader->next_batch(&read_count, dst);
    ASSERT_EQ(read_count, N);
    ASSERT_EQ(dst->size(), N);

    ColumnInt32& dst_col = reinterpret_cast<ColumnInt32&>(*dst);

    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(src_nums[i], dst_col.get(i));
    }

    // ######################################## ColumnReader ########################################
}

TEST(ColumnWriterReaderTest, Int64ColumnWriterReaderTest) {
    const size_t N = 1000000;
    // ######################################## ColumnWriter ########################################

    io::FileWriterPtr file_writer = generate_file_writer();
    ColumnMetaSPtr meta = std::make_shared<ColumnMeta>();
    meta->_column_id = 0;
    meta->_type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_TIMESTAMP);
    meta->_encoding_type = EncodingType::PLAIN_ENCODING;
    meta->_compression_type = CompressionType::NO_COMPRESSION;
    std::unique_ptr<ColumnWriter> column_writer = std::make_unique<ColumnWriter>(meta, file_writer.get());
    MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_TIMESTAMP, "src");
    std::vector<int64_t> src_nums;

    for (size_t i = 0; i < N; ++i) {
        src_nums.push_back(generate_random_timestamp());
    }

    const uint8_t* src_ptr = reinterpret_cast<const uint8_t*>(src_nums.data());
    column_writer->append_data(&src_ptr, N);
    column_writer->write_column_data();
    column_writer->write_column_index();
    file_writer->finalize();
    file_writer->close();

    // ######################################## ColumnWriter ########################################

    // ######################################## ColumnReader ########################################

    io::FileReaderSPtr file_reader = generate_file_reader();
    std::unique_ptr<ColumnReader> column_reader = std::make_unique<ColumnReader>(meta, N, file_reader);
    column_reader->seek_to_first();
    size_t read_count = N;
    column_reader->next_batch(&read_count, dst);
    ASSERT_EQ(read_count, N);
    ASSERT_EQ(dst->size(), N);

    ColumnInt64& dst_col = reinterpret_cast<ColumnInt64&>(*dst);

    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(src_nums[i], dst_col.get(i));
    }

    // ######################################## ColumnReader ########################################
}

TEST(ColumnWriterReaderTest, Float64ColumnWriterReaderTest) {
    const size_t N = 1000000;
    // ######################################## ColumnWriter ########################################

    io::FileWriterPtr file_writer = generate_file_writer();
    ColumnMetaSPtr meta = std::make_shared<ColumnMeta>();
    meta->_column_id = 0;
    meta->_type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_DOUBLE_FLOAT);
    meta->_encoding_type = EncodingType::PLAIN_ENCODING;
    meta->_compression_type = CompressionType::NO_COMPRESSION;
    std::unique_ptr<ColumnWriter> column_writer = std::make_unique<ColumnWriter>(meta, file_writer.get());
    MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_DOUBLE_FLOAT, "src");
    std::vector<double_t> src_nums;

    for (size_t i = 0; i < N; ++i) {
        src_nums.push_back(generate_random_float64());
    }

    const uint8_t* src_ptr = reinterpret_cast<const uint8_t*>(src_nums.data());
    column_writer->append_data(&src_ptr, N);
    column_writer->write_column_data();
    column_writer->write_column_index();
    file_writer->finalize();
    file_writer->close();

    // ######################################## ColumnWriter ########################################

    // ######################################## ColumnReader ########################################

    io::FileReaderSPtr file_reader = generate_file_reader();
    std::unique_ptr<ColumnReader> column_reader = std::make_unique<ColumnReader>(meta, N, file_reader);
    column_reader->seek_to_first();
    size_t read_count = N;
    column_reader->next_batch(&read_count, dst);
    ASSERT_EQ(read_count, N);
    ASSERT_EQ(dst->size(), N);

    ColumnFloat64& dst_col = reinterpret_cast<ColumnFloat64&>(*dst);

    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(src_nums[i], dst_col.get(i));
    }

    // ######################################## ColumnReader ########################################
}

TEST(ColumnWriterReaderTest, StringColumnWriterReaderTest) {
    const size_t N = 100000;
    // ######################################## ColumnWriter ########################################

    io::FileWriterPtr file_writer = generate_file_writer();
    ColumnMetaSPtr meta = std::make_shared<ColumnMeta>();
    meta->_column_id = 0;
    meta->_type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_STRING);
    meta->_encoding_type = EncodingType::PLAIN_ENCODING;
    meta->_compression_type = CompressionType::NO_COMPRESSION;
    std::unique_ptr<ColumnWriter> column_writer = std::make_unique<ColumnWriter>(meta, file_writer.get());
    MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_STRING, "src");
    std::vector<std::string> ss;
    std::vector<Slice> slices;

    for (size_t i = 0; i < N; ++i) {
        ss.push_back(generate_random_string(generate_random_int32() % 1000));

    }

    for (size_t i = 0; i < N; ++i) {
        slices.push_back(ss[i]);
        ASSERT_EQ(ss[i], slices[i].to_string());
    }

    const uint8_t* src_ptr = reinterpret_cast<const uint8_t*>(slices.data());
    column_writer->append_data(&src_ptr, N);
    column_writer->write_column_data();
    column_writer->write_column_index();
    file_writer->finalize();
    file_writer->close();

    // ######################################## ColumnWriter ########################################

    // ######################################## ColumnReader ########################################

    io::FileReaderSPtr file_reader = generate_file_reader();
    std::unique_ptr<ColumnReader> column_reader = std::make_unique<ColumnReader>(meta, N, file_reader);
    column_reader->seek_to_first();
    size_t read_count = N;
    column_reader->next_batch(&read_count, dst);
    ASSERT_EQ(read_count, N);
    ASSERT_EQ(dst->size(), N);

    ColumnString& dst_col = reinterpret_cast<ColumnString&>(*dst);

    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(ss[i], dst_col.get(i));
    }

    // ######################################## ColumnReader ########################################
}

TEST(ColumnWriterReaderTest, SeekInt32ColumnWriterReaderTest) {
    const size_t N = 1000000;
    // ######################################## ColumnWriter ########################################

    io::FileWriterPtr file_writer = generate_file_writer();
    ColumnMetaSPtr meta = std::make_shared<ColumnMeta>();
    meta->_column_id = 0;
    meta->_type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_INTEGER);
    meta->_encoding_type = EncodingType::PLAIN_ENCODING;
    meta->_compression_type = CompressionType::NO_COMPRESSION;
    std::unique_ptr<ColumnWriter> column_writer = std::make_unique<ColumnWriter>(meta, file_writer.get());
    MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_INTEGER, "src");
    std::vector<int32_t> src_nums;

    for (size_t i = 0; i < N; ++i) {
        src_nums.push_back(generate_random_int32());
    }

    const uint8_t* src_ptr = reinterpret_cast<const uint8_t*>(src_nums.data());
    column_writer->append_data(&src_ptr, N);
    column_writer->write_column_data();
    column_writer->write_column_index();
    file_writer->finalize();
    file_writer->close();

    // ######################################## ColumnWriter ########################################

    // ######################################## ColumnReader ########################################

    io::FileReaderSPtr file_reader = generate_file_reader();
    std::unique_ptr<ColumnReader> column_reader = std::make_unique<ColumnReader>(meta, N, file_reader);
    ordinal_t rand_ordinal = generate_random_int32() % N;
    column_reader->seek_to_ordinal(rand_ordinal);
    size_t read_count = 1024;
    size_t actual_read_count = read_count;
    column_reader->next_batch(&actual_read_count, dst);
    ASSERT_EQ(actual_read_count, std::min(read_count, N - rand_ordinal));

    ColumnInt32& dst_col = reinterpret_cast<ColumnInt32&>(*dst);
    ASSERT_EQ(actual_read_count, dst_col.size());

    for (size_t i = 0; i < actual_read_count; ++i) {
        ASSERT_EQ(src_nums[rand_ordinal + i], dst_col.get(i));
    }

    // ######################################## ColumnReader ########################################
}

TEST(ColumnWriterReaderTest, SeekInt64ColumnWriterReaderTest) {
    const size_t N = 1000000;
    // ######################################## ColumnWriter ########################################

    io::FileWriterPtr file_writer = generate_file_writer();
    ColumnMetaSPtr meta = std::make_shared<ColumnMeta>();
    meta->_column_id = 0;
    meta->_type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_TIMESTAMP);
    meta->_encoding_type = EncodingType::PLAIN_ENCODING;
    meta->_compression_type = CompressionType::NO_COMPRESSION;
    std::unique_ptr<ColumnWriter> column_writer = std::make_unique<ColumnWriter>(meta, file_writer.get());
    MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_TIMESTAMP, "src");
    std::vector<int64_t> src_nums;

    for (size_t i = 0; i < N; ++i) {
        src_nums.push_back(generate_random_timestamp());
    }

    const uint8_t* src_ptr = reinterpret_cast<const uint8_t*>(src_nums.data());
    column_writer->append_data(&src_ptr, N);
    column_writer->write_column_data();
    column_writer->write_column_index();
    file_writer->finalize();
    file_writer->close();

    // ######################################## ColumnWriter ########################################

    // ######################################## ColumnReader ########################################

    io::FileReaderSPtr file_reader = generate_file_reader();
    std::unique_ptr<ColumnReader> column_reader = std::make_unique<ColumnReader>(meta, N, file_reader);
    ordinal_t rand_ordinal = generate_random_int32() % N;
    column_reader->seek_to_ordinal(rand_ordinal);
    size_t read_count = 1024;
    size_t actual_read_count = read_count;
    column_reader->next_batch(&actual_read_count, dst);
    ASSERT_EQ(actual_read_count, std::min(read_count, N - rand_ordinal));

    ColumnInt64& dst_col = reinterpret_cast<ColumnInt64&>(*dst);
    ASSERT_EQ(actual_read_count, dst_col.size());

    for (size_t i = 0; i < actual_read_count; ++i) {
        ASSERT_EQ(src_nums[rand_ordinal + i], dst_col.get(i));
    }

    // ######################################## ColumnReader ########################################
}

TEST(ColumnWriterReaderTest, SeekFloat64ColumnWriterReaderTest) {
    const size_t N = 1000000;
    // ######################################## ColumnWriter ########################################

    io::FileWriterPtr file_writer = generate_file_writer();
    ColumnMetaSPtr meta = std::make_shared<ColumnMeta>();
    meta->_column_id = 0;
    meta->_type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_DOUBLE_FLOAT);
    meta->_encoding_type = EncodingType::PLAIN_ENCODING;
    meta->_compression_type = CompressionType::NO_COMPRESSION;
    std::unique_ptr<ColumnWriter> column_writer = std::make_unique<ColumnWriter>(meta, file_writer.get());
    MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_DOUBLE_FLOAT, "src");
    std::vector<double_t> src_nums;

    for (size_t i = 0; i < N; ++i) {
        src_nums.push_back(generate_random_float64());
    }

    const uint8_t* src_ptr = reinterpret_cast<const uint8_t*>(src_nums.data());
    column_writer->append_data(&src_ptr, N);
    column_writer->write_column_data();
    column_writer->write_column_index();
    file_writer->finalize();
    file_writer->close();

    // ######################################## ColumnWriter ########################################

    // ######################################## ColumnReader ########################################

    io::FileReaderSPtr file_reader = generate_file_reader();
    std::unique_ptr<ColumnReader> column_reader = std::make_unique<ColumnReader>(meta, N, file_reader);
    ordinal_t rand_ordinal = generate_random_int32() % N;
    column_reader->seek_to_ordinal(rand_ordinal);
    size_t read_count = 1024;
    size_t actual_read_count = read_count;
    column_reader->next_batch(&actual_read_count, dst);
    ASSERT_EQ(actual_read_count, std::min(read_count, N - rand_ordinal));

    ColumnFloat64& dst_col = reinterpret_cast<ColumnFloat64&>(*dst);
    ASSERT_EQ(actual_read_count, dst_col.size());

    for (size_t i = 0; i < actual_read_count; ++i) {
        ASSERT_EQ(src_nums[rand_ordinal + i], dst_col.get(i));
    }

    // ######################################## ColumnReader ########################################
}

TEST(ColumnWriterReaderTest, SeekStringColumnWriterReaderTest) {
    const size_t N = 100000;
    // ######################################## ColumnWriter ########################################

    io::FileWriterPtr file_writer = generate_file_writer();
    ColumnMetaSPtr meta = std::make_shared<ColumnMeta>();
    meta->_column_id = 0;
    meta->_type = DataTypeFactory::instance().get_column_data_type(COLUMN_TYPE_STRING);
    meta->_encoding_type = EncodingType::PLAIN_ENCODING;
    meta->_compression_type = CompressionType::NO_COMPRESSION;
    std::unique_ptr<ColumnWriter> column_writer = std::make_unique<ColumnWriter>(meta, file_writer.get());
    MutableColumnSPtr dst = vectorized::ColumnFactory::instance().create_column(COLUMN_TYPE_STRING, "src");
    std::vector<std::string> ss;
    std::vector<Slice> slices;

    for (size_t i = 0; i < N; ++i) {
        ss.push_back(generate_random_string(generate_random_int32() % 1000));

    }

    for (size_t i = 0; i < N; ++i) {
        slices.push_back(ss[i]);
        ASSERT_EQ(ss[i], slices[i].to_string());
    }

    const uint8_t* src_ptr = reinterpret_cast<const uint8_t*>(slices.data());
    column_writer->append_data(&src_ptr, N);
    column_writer->write_column_data();
    column_writer->write_column_index();
    file_writer->finalize();
    file_writer->close();

    // ######################################## ColumnWriter ########################################

    // ######################################## ColumnReader ########################################

    io::FileReaderSPtr file_reader = generate_file_reader();
    std::unique_ptr<ColumnReader> column_reader = std::make_unique<ColumnReader>(meta, N, file_reader);
    ordinal_t rand_ordinal = generate_random_int32() % N;
    column_reader->seek_to_ordinal(rand_ordinal);
    size_t read_count = 1024;
    size_t actual_read_count = read_count;
    column_reader->next_batch(&actual_read_count, dst);
    ASSERT_EQ(actual_read_count, std::min(read_count, N - rand_ordinal));

    ColumnString& dst_col = reinterpret_cast<ColumnString&>(*dst);
    ASSERT_EQ(actual_read_count, dst_col.size());

    for (size_t i = 0; i < actual_read_count; ++i) {
        ASSERT_EQ(ss[rand_ordinal + i], dst_col.get(i));
    }

    // ######################################## ColumnReader ########################################
}

}

