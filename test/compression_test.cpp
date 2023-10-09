#include <gtest/gtest.h>
#include "compression/compressor.h"
#include "compression/integer_compression.h"
#include <random>

namespace LindormContest::test {

template<typename T>
void verifyResult(std::vector<T>& input, const char* result){
    auto length = input.size();
    auto* base = reinterpret_cast<T*>(const_cast<char *>(result));
    for(auto i = 0 ;i < length;++i){
        assert(input[i] == *base);
//                GTEST_LOG_(INFO) << input[i] << " " <<  *base;
        if(i != length - 1)base++;
    }
}

TEST(Compression, simple8b_int_test) {
    static constexpr size_t BIG_INT = 100000;
    std::vector<int> input;
    for(auto i = 0 ;i < 20'000;++i){
        auto num = rand() % 1000 - 500;
        //        GTEST_LOG_(INFO) << num;
        input.emplace_back(num);
    }
    auto length = input.size();
    uint32_t uncompressSize = length * sizeof(int);

    LindormContest::compression::CompressionSimple8b compressionSimple8B(4);
    // pre-allocate a large size
    char* origin = reinterpret_cast<char *>(input.data());
    char *compress = reinterpret_cast<char *>(malloc(uncompressSize));

    uint64_t compress_size = compressionSimple8B.compress(origin,uncompressSize,compress);

    char *recover = reinterpret_cast<char*>(malloc(BIG_INT));

    compressionSimple8B.decompress(compress,compress_size,recover,uncompressSize);

    verifyResult<int>(input,recover);

    free(recover);
    free(compress);
    GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
    GTEST_LOG_(INFO) << "compress ratio: " << compress_size * 1.0 / uncompressSize;
}

TEST(Compression, rle_int_test) {
    static constexpr size_t BIG_INT = 100000;
    std::vector<int> input;
    for(auto i = 0 ;i < 20'000;++i){
        auto num = 10;
        //        GTEST_LOG_(INFO) << num;
        input.emplace_back(num);
    }
    auto length = input.size();
    uint32_t uncompressSize = length * sizeof(int);

    LindormContest::compression::CompressionSimple8b compressionSimple8B(4);
    // pre-allocate a large size
    char* origin = reinterpret_cast<char *>(input.data());
    char *compress = reinterpret_cast<char *>(malloc(uncompressSize));

    uint64_t compress_size = compressionSimple8B.compress(origin,uncompressSize,compress, true);

    char *recover = reinterpret_cast<char*>(malloc(BIG_INT));

    compressionSimple8B.decompress(compress,compress_size,recover,uncompressSize, true);

    verifyResult<int>(input,recover);

    free(recover);
    free(compress);
    GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
    GTEST_LOG_(INFO) << "compress ratio: " << compress_size * 1.0 / uncompressSize;
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