#pragma once

#include "compression/utils/unaligned.h"
#include "compression/utils/BitHelpers.h"
#include <bitset>
#include <cstring>
#include <algorithm>
#include <type_traits>
#include <cassert>
#include <stdexcept>

namespace LindormContest::compression {

    class CompressionCodecGorilla {
    public:
        explicit CompressionCodecGorilla(uint8_t data_bytes_size_);

        virtual ~CompressionCodecGorilla() = default;

        uint32_t compress(const char *source, uint32_t source_size, char *dest) const;

        uint8_t decompress(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) const;

        uint32_t getMaxCompressedDataSize(uint32_t uncompressed_size) const;

    private:
        const uint8_t data_bytes_size;
    };

}
