#include "compression/chimp_compression.h"


namespace LindormContest::compression {

uint32_t CompressionCodecChimp::getMaxCompressedDataSize(uint32_t uncompressed_size) const {
    return LindormContest::Storage::BLOCK_SIZE + 1;
}
}
