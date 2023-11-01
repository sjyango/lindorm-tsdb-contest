#include <gtest/gtest.h>
#include "compression/compressor.h"
#include "compression/integer_compression.h"
#include <random>
#include "../source/chimp/include/chimp_compress.hpp"
#include "../source/chimp/include/chimp_scan.hpp"
#include "../source/brotli/encode.h"
#include "../source/brotli/decode.h"

namespace LindormContest::test {

    template<typename T>
    void verifyResult(std::vector<T> &input, const char *result) {
        auto length = input.size();
        auto *base = reinterpret_cast<T *>(const_cast<char *>(result));
        for (auto i = 0; i < length; ++i) {
            assert(input[i] == *base);
            //                GTEST_LOG_(INFO) << input[i] << " " <<  *base;
            if (i != length - 1) base++;
        }
    }

    static double_t generate_random_float64() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<double> dis(-1000000.0, 1000000.0);
        return dis(gen);
    }

    static int32_t generate_random_int32() {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<int32_t> dis(13000, 13000 + 9985);
        return dis(gen);
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

    TEST(Compression, brotli_string_test) {
        const size_t N = 100000;
        std::string encode_data = generate_random_string(N);
        std::string decode_data;
        decode_data.resize(N);
        std::unique_ptr<char[]> compress_data = std::make_unique<char[]>(N * 2);
        size_t encode_size;
        bool encode_res = BrotliEncoderCompress(BROTLI_DEFAULT_QUALITY, BROTLI_DEFAULT_WINDOW, BROTLI_DEFAULT_MODE,
                              N, reinterpret_cast<const uint8_t *>(encode_data.c_str()), &encode_size,
                              reinterpret_cast<uint8_t *>(compress_data.get()));
        ASSERT_TRUE(encode_res);

        size_t decode_size;
        auto decode_res = BrotliDecoderDecompress(encode_size, reinterpret_cast<const uint8_t *>(compress_data.get()), &decode_size,
                                                  reinterpret_cast<uint8_t *>(decode_data.data()));
        ASSERT_EQ(decode_res, BROTLI_DECODER_RESULT_SUCCESS);
        ASSERT_EQ(decode_size, N);
        ASSERT_EQ(encode_data, decode_data);
        GTEST_LOG_(INFO) << "original size: " << N << "; compress size: " << encode_size;
    }

    TEST(Compression, simple8b_int_test) {
        static constexpr size_t BIG_INT = 100000;
        std::vector<int> input;
        for (auto i = 0; i < 20'000; ++i) {
            auto num = rand() % 1000 - 500;
            //        GTEST_LOG_(INFO) << num;
            input.emplace_back(num);
        }
        auto length = input.size();
        uint32_t uncompressSize = length * sizeof(int);

        LindormContest::compression::CompressionSimple8b compressionSimple8B(4);
        // pre-allocate a large size
        char *origin = reinterpret_cast<char *>(input.data());
        char *compress = reinterpret_cast<char *>(malloc(uncompressSize));

        uint64_t compress_size = compressionSimple8B.compress(origin, uncompressSize, compress);

        char *recover = reinterpret_cast<char *>(malloc(BIG_INT));

        compressionSimple8B.decompress(compress, compress_size, recover, uncompressSize);
    }

    TEST(Compression, rle_int_test) {
        static constexpr size_t BIG_INT = 100000;
        std::vector<int> input;
        for (auto i = 0; i < 20'000; ++i) {
            auto num = 10;
            //        GTEST_LOG_(INFO) << num;
            input.emplace_back(num);
        }
        auto length = input.size();
        uint32_t uncompressSize = length * sizeof(int);

        LindormContest::compression::CompressionSimple8b compressionSimple8B(4);
        // pre-allocate a large size
        char *origin = reinterpret_cast<char *>(input.data());
        char *compress = reinterpret_cast<char *>(malloc(uncompressSize));

        uint64_t compress_size = compressionSimple8B.compress(origin, uncompressSize, compress);

        char *recover = reinterpret_cast<char *>(malloc(BIG_INT));

        compressionSimple8B.decompress(compress, compress_size, recover, uncompressSize);

        verifyResult<int>(input, recover);

        free(recover);
        free(compress);
        GTEST_LOG_(INFO) << "original size: " << uncompressSize
                         << "; compress size: " << compress_size;
        GTEST_LOG_(INFO) << "compress ratio: " << compress_size * 1.0 / uncompressSize;
    }

    TEST(Compression, chimp_double_test) {
        const size_t N = 2000;
        const size_t BIG_INT = BLOCK_SIZE;
        std::vector<double> input;

        for (size_t i = 0; i < N; ++i) {
            input.emplace_back(generate_random_float64());
        }

        uint32_t uncompressSize = N * sizeof(double);

        char *origin = reinterpret_cast<char *>(input.data());
        char *compress = reinterpret_cast<char *>(malloc(BIG_INT));
        char *gorilla_compress = reinterpret_cast<char *>(malloc(BIG_INT));
        size_t compressSize, compressGorilla;
        compressSize = LindormContest::compression::compress_double_chimp(origin, uncompressSize, compress);
        compressGorilla = LindormContest::compression::compress_double_gorilla(origin, uncompressSize, gorilla_compress);
        GTEST_LOG_(INFO) << "compress size: " << compressSize;

        char *recover = reinterpret_cast<char *>(malloc(BIG_INT));

        auto newDest = LindormContest::compression::decompress_double_chimp(compress, compressSize, recover, uncompressSize);

        verifyResult<double>(input, reinterpret_cast<const char *>(newDest));
        //
        free(recover);
        free(compress);
        free(gorilla_compress);
        GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compressSize;
        GTEST_LOG_(INFO) << "chimp compress ratio: " << compressSize * 1.0 / uncompressSize;
        GTEST_LOG_(INFO) << "gorilla compress ratio: " << compressGorilla * 1.0 / uncompressSize;
    }

    TEST(Compression, gorilla_int_test) {
       static constexpr size_t N = 2000;
       std::vector<int32_t> src;

       for (size_t i = 0; i < N; ++i) {
           src.push_back(generate_random_int32());
       }

       std::unique_ptr<char[]> dst1 = std::make_unique<char[]>(N * 4);
       std::unique_ptr<char[]> dst2 = std::make_unique<char[]>(N * 4);
       std::unique_ptr<char[]> dst3 = std::make_unique<char[]>(N * 4);

       uint32_t compress_size1 = compression::compress_int32_gorilla(reinterpret_cast<const char *>(src.data()), N * 4, dst1.get());
       // uint32_t compress_size2 = compression::compress_int32_fastpfor(reinterpret_cast<const uint32_t *>(src.data()), N * 4,
       //                                                                reinterpret_cast<uint32_t *>(dst2.get()));
       uint32_t compress_size3 = compression::compress_int32_simple8b(reinterpret_cast<const char *>(src.data()), N * 4, dst3.get());

        INFO_LOG("uncompress size: %lu, compress size: %d", N * 4, compress_size1)
        // INFO_LOG("uncompress size: %lu, compress size: %d", N * 4, compress_size2)
        INFO_LOG("uncompress size: %lu, compress size: %d", N * 4, compress_size3)
    }
}