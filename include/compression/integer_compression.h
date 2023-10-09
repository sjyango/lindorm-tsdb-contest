#pragma once

namespace LindormContest::compression {

class CompressionSimple8b {
public:
    explicit CompressionSimple8b(uint8_t data_bytes):data_bytes_size(data_bytes){};

    virtual ~CompressionSimple8b() = default;

    uint64_t compress(const char *source, uint64_t source_size, char *dest) const;
    
    void decompress(const char *source, uint64_t source_size, char *dest, uint64_t uncompressed_size) const;

private:
    template <typename T>
    void ZigZagEncode(T *input, uint64_t length) const;
    
    template <typename T>
    void ZigZagDecode(T *input, uint64_t length) const;
            
private:
    const uint8_t data_bytes_size;
};

template <typename T>
void CompressionSimple8b::ZigZagEncode(T *input, uint64_t length) const{
    T shift = (sizeof(T) * 8) - 1; // sizeof * 8 = # of bits
    for (uint64_t i = 0; i < length; i++)
    {
        input[i] = (input[i] << 1LL) ^ (input[i] >> shift);
    }
    return;
}

template <typename T>
void CompressionSimple8b::ZigZagDecode(T *input, uint64_t length) const{
    for (uint64_t i = 0; i < length; i++)
    {
        input[i] = (input[i] >> 1LL) ^ -(input[i] & 1LL);
    }
    return;
}

}