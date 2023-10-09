#include <stdint-gcc.h>

#include <cstring>

#include "compression/integer_compression.h"
#include "compression/simple8b/simple8b.h"

namespace LindormContest::compression {
uint64_t CompressionSimple8b::compress(const char *source, uint64_t source_size, char *dest) const{
    //init
    uint32_t *out = reinterpret_cast<uint32_t*>(dest);
    size_t length = source_size / data_bytes_size;

    //zig-zag
    int32_t *src = reinterpret_cast<int32_t *>(malloc(source_size));
    memcpy(src,source,source_size);
    ZigZagEncode<int32_t>(src,length);
    
    //simple-8b
    uint32_t *UnsignedSrc = reinterpret_cast<uint32_t *>(src);
    size_t compressSize = -1;
    encodeArray<true>(UnsignedSrc,length,out,compressSize);
    free(src);
    // compressSize uint32_t numbers
    return compressSize * 4;
}

// source_size is unused
void CompressionSimple8b::decompress(const char *source, uint64_t source_size, char *dest, uint64_t uncompressed_size) const{
    const uint32_t *src = reinterpret_cast<const uint32_t *>(source);
    uint64_t length = uncompressed_size / data_bytes_size;
    uint32_t *out = reinterpret_cast<uint32_t*>(dest);
    decodeArray<true>(src,source_size,out,length);
    
    ZigZagDecode<uint32_t>(out,length);
}

}