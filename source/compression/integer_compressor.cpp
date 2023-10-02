#include <stdint-gcc.h>

#include "../source/compression/simple8b/simple8b.h"
#include "compression/integer_compression.h"

namespace LindormContest::compression {
uint64_t CompressionSimple8b::compress(char *source, uint64_t source_size, char *dest) const{
    uint64_t *out = reinterpret_cast<uint64_t*>(dest);
    uint64_t length = source_size / data_bytes_size;
    if(data_bytes_size == 2){
        uint16_t *src = reinterpret_cast<uint16_t*>(source);
        return Simple8bEncode<uint16_t>(src, length, out);
    }
    else if(data_bytes_size == 4){
        uint32_t *src = reinterpret_cast<uint32_t*>(source);
        return Simple8bEncode<uint32_t>(src, length, out);
    }
    else if(data_bytes_size == 8){
        uint64_t *src = reinterpret_cast<uint64_t*>(source);
        return Simple8bEncode<uint64_t>(src, length, out);
    }
    return -1;
}

// source_size is unused
uint64_t CompressionSimple8b::decompress(char *source, uint64_t source_size, char *dest, uint64_t uncompressed_size) const{
    uint64_t *src = reinterpret_cast<uint64_t*>(source);
    uint64_t length = uncompressed_size / data_bytes_size;
    if(data_bytes_size == 2){
        uint16_t *out = reinterpret_cast<uint16_t*>(dest);
        return Simple8bDecode<uint16_t>(src, length, out);
    }
    else if(data_bytes_size == 4){
        uint32_t *out = reinterpret_cast<uint32_t*>(dest);
        return Simple8bDecode<uint32_t>(src, length, out);
    }
    else if(data_bytes_size == 8){
        uint64_t *out = reinterpret_cast<uint64_t*>(dest);
        return Simple8bDecode<uint64_t>(src, length, out);
    }
    return -1;
}

}