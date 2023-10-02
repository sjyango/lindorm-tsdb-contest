#pragma once

namespace LindormContest::compression {

class CompressionSimple8b {
public:
    explicit CompressionSimple8b(uint8_t data_bytes):data_bytes_size(data_bytes){};

    virtual ~CompressionSimple8b() = default;

    uint64_t compress(char *source, uint64_t source_size, char *dest) const;
    
    uint64_t decompress(char *source, uint64_t source_size, char *dest, uint64_t uncompressed_size) const;

private:
    const uint8_t data_bytes_size;
};

}