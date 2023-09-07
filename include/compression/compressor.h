#pragma once

#include "compression/float_compression.h"
#include "compression/string_compressor.h"

namespace LindormContest::compression {

    static uint32_t compress_float(const char *source, uint32_t source_size, char *dest) {
        static CompressionCodecGorilla compressionCodecGorilla(8);
        return compressionCodecGorilla.compress(source, source_size, dest);
    }

    static char * decompress_float(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) {
        static CompressionCodecGorilla compressionCodecGorilla(8);
        return dest + compressionCodecGorilla.decompress(source, source_size, dest, uncompressed_size);
    }

    static uint32_t compress_int32(const char *source, uint32_t source_size, char *dest) {
        static CompressionCodecGorilla compressionCodecGorilla(4);
        return compressionCodecGorilla.compress(source, source_size, dest);
    }

    static char * decompress_int32(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) {
        static CompressionCodecGorilla compressionCodecGorilla(4);
        return dest + compressionCodecGorilla.decompress(source, source_size, dest, uncompressed_size);
    }

    static uint32_t compress_int64(const char *source, uint32_t source_size, char *dest) {
        static CompressionCodecGorilla compressionCodecGorilla(8);
        return compressionCodecGorilla.compress(source, source_size, dest);
    }

    static char * decompress_int64(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) {
        static CompressionCodecGorilla compressionCodecGorilla(8);
        return dest + compressionCodecGorilla.decompress(source, source_size, dest, uncompressed_size);
    }

    static uint32_t compress_string(const char *source, uint32_t source_size, char *dest) {
        static CompressionCodecZSTD compressionCodecZstd(3);
        return compressionCodecZstd.compress(source, source_size, dest);
    }

    static void decompress_string(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) {
        static CompressionCodecZSTD compressionCodecZstd(3);
        compressionCodecZstd.decompress(source, source_size, dest, uncompressed_size);
    }

}
