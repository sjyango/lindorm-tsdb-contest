#include "compression/string_compressor.h"

namespace LindormContest::compression {

    uint32_t CompressionCodecZSTD::compress(const char *source, uint32_t source_size, char *dest) const {
        ZSTD_CCtx *cctx = ZSTD_createCCtx();
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, 1);
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_strategy, ZSTD_fast);
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

    uint32_t CompressionCodecBrotli::compress(const char *source, uint32_t source_size, char *dest) const {
        size_t encode_size;
        bool encode_res = BrotliEncoderCompress(5, BROTLI_DEFAULT_WINDOW, BROTLI_DEFAULT_MODE,
                                                source_size, reinterpret_cast<const uint8_t *>(source), &encode_size,
                                                reinterpret_cast<uint8_t *>(dest));
        assert(encode_res);
        return encode_size;
    }

    void CompressionCodecBrotli::decompress(const char *source, uint32_t source_size,
                                            char *dest, uint32_t uncompressed_size) const {
        size_t decode_size;
        auto decode_res = BrotliDecoderDecompress(source_size, reinterpret_cast<const uint8_t *>(source), &decode_size,
                                                  reinterpret_cast<uint8_t *>(dest));
        assert(decode_size == uncompressed_size);
        assert(decode_res == BROTLI_DECODER_RESULT_SUCCESS);
    }
}