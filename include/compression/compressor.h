#pragma once

#include "compression/float_compression.h"
#include "compression/string_compressor.h"
#include "compression/integer_compression.h"

namespace LindormContest::compression {

    static uint32_t compress_double(const char *source, uint32_t source_size, char *dest) {
        static CompressionCodecGorilla compressionCodecGorilla(8);
        return compressionCodecGorilla.compress(source, source_size, dest);
    }

    static char * decompress_double(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) {
        static CompressionCodecGorilla compressionCodecGorilla(8);
        return dest + compressionCodecGorilla.decompress(source, source_size, dest, uncompressed_size);
    }

    static uint32_t compress_int16(const char *source, uint32_t source_size, char *dest) {
        static CompressionCodecGorilla compressionCodecGorilla(2);
        return compressionCodecGorilla.compress(source, source_size, dest);
    }

    static char * decompress_int16(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) {
        static CompressionCodecGorilla compressionCodecGorilla(2);
        return dest + compressionCodecGorilla.decompress(source, source_size, dest, uncompressed_size);
    }

    static uint32_t compress_int32_simple8b(const char *source, uint32_t source_size, char *dest) {
        static CompressionSimple8b compressionCodecSimple8b(4);
        return compressionCodecSimple8b.compress(source, source_size, dest);
    }

    static char * decompress_int32_simple8b(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) {
        static CompressionSimple8b compressionCodecSimple8b(4);
        compressionCodecSimple8b.decompress(source, source_size, dest, uncompressed_size);
        return dest;
    }

    static uint32_t compress_int32_rle(const char *source, uint32_t source_size, char *dest) {
        static CompressionRLE compressionCodecRLE(4);
        return compressionCodecRLE.compress(source, source_size, dest);
    }

    static char * decompress_int32_rle(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) {
        static CompressionRLE compressionCodecRLE(4);
        compressionCodecRLE.decompress(source, source_size, dest, uncompressed_size);
        return dest;
    }

    static uint32_t compress_int64(const char *source, uint32_t source_size, char *dest) {
        static CompressionCodecGorilla compressionCodecGorilla(8);
        return compressionCodecGorilla.compress(source, source_size, dest);
    }

    static char * decompress_int64(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) {
        static CompressionCodecGorilla compressionCodecGorilla(8);
        return dest + compressionCodecGorilla.decompress(source, source_size, dest, uncompressed_size);
    }

    static uint32_t compress_string_zstd(const char *source, uint32_t source_size, char *dest) {
        static CompressionCodecZSTD compressionCodecZstd(3);
        return compressionCodecZstd.compress(source, source_size, dest);
    }

    static void decompress_string_zstd(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) {
        static CompressionCodecZSTD compressionCodecZstd(3);
        compressionCodecZstd.decompress(source, source_size, dest, uncompressed_size);
    }

}
