#pragma once
#include <cstdint>
namespace LindormContest::compression {

class CompressionBitPack {
public:
    CompressionBitPack() = default;

    virtual ~CompressionBitPack() = default;

    uint32_t compress(const char *source, uint32_t source_size, char *dest) const;

    uint8_t decompress(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) const;
    
};

}