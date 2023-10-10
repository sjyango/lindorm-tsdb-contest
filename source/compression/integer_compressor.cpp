#include <stdint-gcc.h>

#include <cstring>

#include "compression/integer_compression.h"
#include "compression/simple8b/simple8b.h"

namespace LindormContest::compression {
uint64_t CompressionSimple8b::compress(const char *source, uint64_t source_size, char *dest, bool isRLE) const{
    size_t length = source_size / data_bytes_size;
    if(isRLE){
        return tsCompressRLEImp(source, length, dest);
    }
    return tsCompressINTImp(source, length, dest);
}

// source_size is unused
void CompressionSimple8b::decompress(const char *source, uint64_t source_size, char *dest, uint64_t uncompressed_size, bool isRLE) const{
    uint64_t length = uncompressed_size / data_bytes_size;
   if(isRLE){
        tsDecompressRLEImp(source, length, dest);
   }
   else{
        tsDecompressINTImp(source, length, dest);
   }
}

}