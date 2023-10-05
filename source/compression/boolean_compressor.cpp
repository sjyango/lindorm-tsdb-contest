#include "compression/boolean_compression.h"
//#include "../source/compression/bitpack/include/bitpacker/bitpacker.hpp"

namespace LindormContest::compression {
uint32_t CompressionBitPack::compress(const char* source, uint32_t source_size, char* dest) const {
    return -1;
}
uint8_t CompressionBitPack::decompress(const char* source, uint32_t source_size, char* dest, uint32_t uncompressed_size) const {
    return -1;
}
}