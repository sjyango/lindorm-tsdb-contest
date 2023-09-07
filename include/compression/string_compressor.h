#pragma once

#include <cstdint>
#include <memory>

#include "../source/zstd/zstd.h"


namespace LindormContest::compression {

    class CompressionCodecZSTD {
    public:

        explicit CompressionCodecZSTD(int level_) : level(level_) {}

        uint32_t compress(const char *source, uint32_t source_size, char *dest) const;

        void decompress(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) const;

    private:
        const int level;

    };

}
