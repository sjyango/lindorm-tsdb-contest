#pragma once

#include "compression/utils/unaligned.h"
#include "compression/utils/BitHelpers.h"
#include <bitset>
#include <cstring>
#include <algorithm>
#include <type_traits>
#include <cassert>
#include <stdexcept>
#include "../source/chimp/include/chimp_compress.hpp"
#include "../source/chimp/include/chimp_scan.hpp"

namespace LindormContest::compression {

    class CompressionCodecChimp {
    public:
        CompressionCodecChimp() = default;

        virtual ~CompressionCodecChimp() = default;

        uint32_t compress(const char *source, uint32_t source_size, char *dest) const;

        void decompress(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) const;

    };

}
