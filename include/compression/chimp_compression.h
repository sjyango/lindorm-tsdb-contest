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
    explicit CompressionCodecChimp(uint8_t data_bytes_size_): data_bytes_size(data_bytes_size_){};

    virtual ~CompressionCodecChimp() = default;

    template <class T>
    uint32_t compress(const char *source, uint32_t source_size, char *dest) const;

    template <class T>
    void decompress(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) const;

    uint32_t getMaxCompressedDataSize(uint32_t uncompressed_size) const;

private:
    const uint8_t data_bytes_size;
};


template<class T>
uint32_t CompressionCodecChimp::compress(const char *source, uint32_t source_size, char *dest) const {
    duckdb::ChimpCompressionState<T> state = duckdb::ChimpCompressionState<T>(reinterpret_cast<uint8_t*>(dest));
    state.Append(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(source)), source_size / data_bytes_size);
    assert(state.GetSegCount() == source_size / data_bytes_size);
    return state.Finalize();
}

template<class T>
void CompressionCodecChimp::decompress(const char *source, uint32_t source_size, char *dest, uint32_t uncompressed_size) const {
    auto segCnt = uncompressed_size / data_bytes_size;
    duckdb::ChimpScanState<T> state = duckdb::ChimpScanState<T>(reinterpret_cast<const uint8_t *>(source),segCnt);
    for (idx_t base_row_index = 0; base_row_index < segCnt; base_row_index += STANDARD_VECTOR_SIZE) {
        idx_t count = duckdb::MinValue<idx_t>(segCnt - base_row_index, STANDARD_VECTOR_SIZE);
        //        scan_state.row_index = segment.start + base_row_index;
        duckdb::ChimpScanPartial<T>(state, count, reinterpret_cast<uint8_t*>(dest));
    }
}

}
