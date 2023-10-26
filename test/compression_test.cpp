#include <gtest/gtest.h>
#include "compression/compressor.h"
#include "compression/integer_compression.h"
#include <random>
#include "../source/chimp/include/chimp_compress.hpp"
#include "../source/chimp/include/chimp_scan.hpp"

extern "C" {
#include "../source/bitpacking/include/simdbitpacking.h"
}

namespace LindormContest::test {

    template<typename T>
    void verifyResult(std::vector<T> &input, const char *result) {
        auto length = input.size();
        auto *base = reinterpret_cast<T *>(const_cast<char *>(result));
        for (auto i = 0; i < length; ++i) {
            assert(input[i] == *base);
//                GTEST_LOG_(INFO) << input[i] << " " <<  *base;
            if (i != length - 1)base++;
        }
    }
//
//    TEST(Compression, simple8b_int_test) {
//        static constexpr size_t BIG_INT = 100000;
//        std::vector<int> input;
//        for (auto i = 0; i < 20'000; ++i) {
//            auto num = rand() % 1000 - 500;
//            //        GTEST_LOG_(INFO) << num;
//            input.emplace_back(num);
//        }
//        auto length = input.size();
//        uint32_t uncompressSize = length * sizeof(int);
//
//        LindormContest::compression::CompressionSimple8b compressionSimple8B(4);
//        // pre-allocate a large size
//        char *origin = reinterpret_cast<char *>(input.data());
//        char *compress = reinterpret_cast<char *>(malloc(uncompressSize));
//
//        uint64_t compress_size = compressionSimple8B.compress(origin, uncompressSize, compress);
//
//        char *recover = reinterpret_cast<char *>(malloc(BIG_INT));
//
//        compressionSimple8B.decompress(compress, compress_size, recover, uncompressSize);
//
//        verifyResult<int>(input, recover);
//
//        free(recover);
//        free(compress);
//        GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
//        GTEST_LOG_(INFO) << "compress ratio: " << compress_size * 1.0 / uncompressSize;
//    }
//
//    TEST(Compression, rle_int_test) {
//        static constexpr size_t BIG_INT = 100000;
//        std::vector<int> input;
//        for (auto i = 0; i < 20'000; ++i) {
//            auto num = 10;
//            //        GTEST_LOG_(INFO) << num;
//            input.emplace_back(num);
//        }
//        auto length = input.size();
//        uint32_t uncompressSize = length * sizeof(int);
//
//        LindormContest::compression::CompressionSimple8b compressionSimple8B(4);
//        // pre-allocate a large size
//        char *origin = reinterpret_cast<char *>(input.data());
//        char *compress = reinterpret_cast<char *>(malloc(uncompressSize));
//
//        uint64_t compress_size = compressionSimple8B.compress(origin, uncompressSize, compress);
//
//        char *recover = reinterpret_cast<char *>(malloc(BIG_INT));
//
//        compressionSimple8B.decompress(compress, compress_size, recover, uncompressSize);
//
//        verifyResult<int>(input, recover);
//
//        free(recover);
//        free(compress);
//        GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
//        GTEST_LOG_(INFO) << "compress ratio: " << compress_size * 1.0 / uncompressSize;
//    }

//    TEST(Compression, BitPackingTest) {
//        size_t k, N = 9999;
//        __m128i * endofbuf;
//        uint32_t * datain = static_cast<uint32_t *>(std::malloc(N * sizeof(uint32_t)));
//        uint8_t * buffer;
//        uint32_t * backbuffer = static_cast<uint32_t *>(malloc(N * sizeof(uint32_t)));
//        uint32_t b;
//
//        for (k = 0; k < N; ++k) {        /* start with k=0, not k=1! */
//            datain[k] = k;
//        }
//
//        b = maxbits_length(datain, N);
//        buffer = static_cast<uint8_t *>(malloc(simdpack_compressedbytes(N, b))); // allocate just enough memory
//        endofbuf = simdpack_length(datain, N, (__m128i *)buffer, b);
//        /* compressed data is stored between buffer and endofbuf using (endofbuf-buffer)*sizeof(__m128i) bytes */
//        /* would be safe to do : buffer = realloc(buffer,(endofbuf-(__m128i *)buffer)*sizeof(__m128i)); */
//        simdunpack_length((const __m128i *)buffer, N, backbuffer, b);
//
//        for (k = 0; k < N; ++k) {
//            ASSERT_EQ(datain[k], backbuffer[k]);
//        }
//    }

    TEST(Compression, chimp_double_test) {
        static constexpr size_t BIG_INT = 10 * LindormContest::Storage::BLOCK_SIZE + 1;
        using CHIMP_TYPE = typename duckdb::ChimpType<double>::type;
//        std::vector<double> input = {1.3, 2.9, 3.1, 5.9};
        std::vector<double> input;
        int A = -5000, B = 5000;
        for (auto i = 0; i < 2000; ++i) {
            float r3 = A + static_cast <float> (rand()) /( static_cast <float> (RAND_MAX/(B-A)));
            input.emplace_back(r3);
        }
        
        auto length = input.size();
        uint32_t uncompressSize = length * sizeof(CHIMP_TYPE);

        // pre-allocate a large size
        uint8_t *origin = reinterpret_cast<uint8_t *>(input.data());
        uint8_t *compress = reinterpret_cast<uint8_t *>(malloc(BIG_INT));
        auto compressSize = duckdb::ChimpCompress<double>(origin,length,compress);
        GTEST_LOG_(INFO) << "compress size: " << compressSize;

        uint8_t *recover = reinterpret_cast<uint8_t *>(malloc(BIG_INT));

        duckdb::DecompressChimp<double>(compress,compressSize,recover);

//        compressionSimple8B.decompress(compress, compress_size, recover, uncompressSize);
//
        verifyResult<double>(input, reinterpret_cast<const char*>(recover));
//
        free(recover);
        free(compress);
//        GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
//        GTEST_LOG_(INFO) << "compress ratio: " << compress_size * 1.0 / uncompressSize;
    }

//TEST(Compression, gorilla_int_test) {
//    static constexpr size_t BIG_INT = 100;
//    std::vector<uint64_t> input = {3,4,4,2,6,7,2,1,100,35};
//    auto length = input.size();
//    uint32_t uncompressSize = length * sizeof(uint64_t);
//
//    // pre-allocate a large size
//    char* origin = reinterpret_cast<char *>(input.data());
//    char *compress = reinterpret_cast<char *>(malloc(uncompressSize));
//
//    uint32_t compress_size = LindormContest::compression::compress_int64(origin,uncompressSize,compress);
//
//    char *recover = reinterpret_cast<char*>(malloc(BIG_INT));
//
//    auto newDest = LindormContest::compression::decompress_int64(compress,compress_size,recover,uncompressSize);
//
//    verifyResult<uint64_t>(input,newDest);
//
//    free(recover);
//    free(compress);
//    GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
//    GTEST_LOG_(INFO) << "compress ratio: " << (uncompressSize - compress_size) * 1.0 / uncompressSize;
//}
}