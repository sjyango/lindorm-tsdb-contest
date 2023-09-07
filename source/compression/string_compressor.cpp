#include "compression/string_compressor.h"

namespace LindormContest::compression {

    uint32_t CompressionCodecZSTD::compress(const char *source, uint32_t source_size, char *dest) const {
        ZSTD_CCtx *cctx = ZSTD_createCCtx();
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level);
        size_t compressed_size = ZSTD_compress2(cctx, dest, ZSTD_compressBound(source_size), source, source_size);
        ZSTD_freeCCtx(cctx);

        if (ZSTD_isError(compressed_size)) {
            throw "Error on compressing";
        }
        return static_cast<uint32_t>(compressed_size);
    }


    void CompressionCodecZSTD::decompress(const char *source, uint32_t source_size, char *dest,
                                          uint32_t uncompressed_size) const {
        size_t res = ZSTD_decompress(dest, uncompressed_size, source, source_size);

        if (ZSTD_isError(res)) {
            throw "Error on decompressing";
        }
    }
}