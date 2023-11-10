#pragma once

#include "base.h"
#include "compression/simple8b/simple8b.h"
#include "../source/pfor/simdfastpfor.h"
#include "../source/pfor/compositecodec.h"
#include "../source/pfor/variablebyte.h"

namespace LindormContest::compression {

    class CompressionSimple8b {
    public:
        explicit CompressionSimple8b(uint8_t data_bytes) : data_bytes_size(data_bytes) {};

        virtual ~CompressionSimple8b() = default;

        size_t compress(const char *source, size_t source_size, char *dest) const;

        void decompress(const char *source, size_t source_size, char *dest, size_t uncompressed_size) const;

    private:
        const uint8_t data_bytes_size;
    };

    class CompressionFastPFor {
    public:
        CompressionFastPFor() {
            _codec = std::shared_ptr<FastPForLib::IntegerCODEC>(
                    new FastPForLib::CompositeCodec<FastPForLib::SIMDFastPFor<8>, FastPForLib::VariableByte>());
        }

        virtual ~CompressionFastPFor() = default;

        size_t compress(const uint32_t *source, size_t source_size, uint32_t *dest) const;

        size_t decompress(const uint32_t *source, size_t source_size, uint32_t *dest) const;

    private:
        std::shared_ptr<FastPForLib::IntegerCODEC> _codec;
    };

    class CompressionRLE {
    public:
        explicit CompressionRLE(uint8_t data_bytes) : data_bytes_size(data_bytes) {};

        virtual ~CompressionRLE() = default;

        size_t compress(const char *source, size_t source_size, char *dest) const;

        void decompress(const char *source, size_t source_size, char *dest, size_t uncompressed_size) const;

    private:
        const uint8_t data_bytes_size;
    };

}