#pragma once

#include <cstdint>
#include <memory>

#include "base.h"

#include "../source/zstd/zstd.h"
#include "../source/brotli/encode.h"
#include "../source/brotli/decode.h"

namespace LindormContest::compression {

    class CompressionCodecZSTD {
    public:
        CompressionCodecZSTD() = default;

        ~CompressionCodecZSTD() = default;

        uint32_t compress(const char *source, uint32_t source_size, char *dest) const;

        void decompress(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) const;
    };

    class CompressionCodecBrotli {
    public:
        CompressionCodecBrotli() = default;

        ~CompressionCodecBrotli() = default;

        uint32_t compress(const char *source, uint32_t source_size, char *dest) const;

        void decompress(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) const;
    };

}
