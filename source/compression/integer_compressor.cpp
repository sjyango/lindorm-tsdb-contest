#include "compression/integer_compression.h"

namespace LindormContest::compression {

    size_t CompressionSimple8b::compress(const char *source, size_t source_size, char *dest) const {
        return tsCompressINTImp(source, source_size / data_bytes_size, dest);
    }

    void CompressionSimple8b::decompress(const char *source, size_t source_size, char *dest, size_t uncompressed_size) const {
        tsDecompressINTImp(source, uncompressed_size / data_bytes_size, dest);
    }

    size_t CompressionFastPFor::compress(const uint32_t *source, size_t source_size, uint32_t *dest) const {
        size_t compress_size = source_size * 2;
        _codec->encodeArray(source, source_size, dest, compress_size);
        return compress_size;
    }

    size_t CompressionFastPFor::decompress(const uint32_t *source, size_t source_size, uint32_t *dest) const {
        size_t uncompress_size = source_size * 2;
        _codec->decodeArray(source, source_size, dest, uncompress_size);
        return uncompress_size;
    }

    size_t CompressionRLE::compress(const char *source, size_t source_size, char *dest) const {
        return tsCompressRLEImp(source, source_size / data_bytes_size, dest);
    }

    void CompressionRLE::decompress(const char *source, size_t source_size, char *dest, size_t uncompressed_size) const {
        tsDecompressRLEImp(source, uncompressed_size / data_bytes_size, dest);
    }

}