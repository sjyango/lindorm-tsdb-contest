// Copyright (c) 2023 vesoft inc. All rights reserved.

#include <gtest/gtest.h>
#include "io/compression_algorithm/gorilla_compressor.h"

namespace LindormContest::test {

TEST(GorillaTest, basic) {
    static constexpr size_t BIG_INT = 100;
    std::vector<double> input = {3.1,3.1,3.1,3.1,3.1};
    auto length = input.size();
    uint32_t uncompressSize = length * sizeof(double);
    INFO_LOG("original size %d", length * sizeof(double ))
    
    io::CompressionCodecGorilla compressor(sizeof(double));
    // pre-allocate a large size
    Slice origin = Slice(reinterpret_cast<char *>(input.data()), uncompressSize);
    std::string *compress = new std::string(BIG_INT,'0');
    
    uint32_t compress_size = compressor.compress(origin,compress);
    INFO_LOG("compress size: %d" , compress_size)
    // modify the real size to slice
    origin._size = compress_size;
    
    char *rec = reinterpret_cast<char*>(malloc(BIG_INT));
    Slice compSlice = Slice(compress->data(),compress_size);
    Slice* recover = new Slice(rec, uncompressSize);
    auto skip = compressor.decompress(compSlice,recover);
    
    auto *data = reinterpret_cast<double *>(recover->mutableData() + skip);
    for(auto i = 0 ;i < length;++i){
        GTEST_LOG_(INFO) << *(data);
        if(i != length - 1)data++;
    }
    delete recover;
    free(rec);
    delete compress;
}
    
}
