#pragma once

#include <cstdint>
#include <memory>

#include "base.h"

#include "../source/zstd/zstd.h"
#include "../source/brotli/encode.h"
#include "../source/brotli/decode.h"
#include "../source/fsst/third_party/fsst.h"

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
    
    class CompressionCodecFsst {
    public:
        CompressionCodecFsst() = default;
        
        ~CompressionCodecFsst() = default;
        
        uint32_t compress(const std::vector<std::string> &source, uint32_t source_size, char* dest);
        
        void decompress(const char *source, uint32_t source_size, std::vector<std::string> &dest, uint32_t uncompressed_size) const;
        
    };

}
