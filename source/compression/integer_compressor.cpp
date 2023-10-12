#include <stdint-gcc.h>

#include <cstring>

#include "compression/integer_compression.h"
#include "compression/simple8b/simple8b.h"

namespace LindormContest::compression {

    uint64_t CompressionSimple8b::compress(const char *source, uint64_t source_size, char *dest) const {
        return tsCompressINTImp(source, source_size / data_bytes_size, dest);
    }

    void CompressionSimple8b::decompress(const char *source, uint64_t source_size, char *dest, uint64_t uncompressed_size) const {
        tsDecompressINTImp(source, uncompressed_size / data_bytes_size, dest);
    }

    uint64_t CompressionRLE::compress(const char *source, uint64_t source_size, char *dest) const {
        return tsCompressRLEImp(source, source_size / data_bytes_size, dest);
    }

    void CompressionRLE::decompress(const char *source, uint64_t source_size, char *dest, uint64_t uncompressed_size) const {
        tsDecompressRLEImp(source, uncompressed_size / data_bytes_size, dest);
    }

}