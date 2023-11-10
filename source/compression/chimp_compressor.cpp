#include "compression/chimp_compression.h"


namespace LindormContest::compression {

    uint32_t CompressionCodecChimp::compress(const char *source, uint32_t source_size, char *dest) const {
        duckdb::ChimpCompressionState<double_t> state = duckdb::ChimpCompressionState<double_t>(reinterpret_cast<uint8_t *>(dest));
        state.Append(const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(source)), source_size / sizeof(double_t));
        assert(state.GetSegCount() == source_size / sizeof(double_t));
        return state.Finalize();
    }

    void CompressionCodecChimp::decompress(const char *source, uint32_t source_size, char *dest,
                                           uint32_t uncompressed_size) const {
        auto segCnt = uncompressed_size / sizeof(double_t);
        duckdb::ChimpScanState<double_t> state = duckdb::ChimpScanState<double_t>(reinterpret_cast<const uint8_t *>(source), segCnt);
        for (idx_t base_row_index = 0; base_row_index < segCnt; base_row_index += STANDARD_VECTOR_SIZE) {
            idx_t count = duckdb::MinValue<idx_t>(segCnt - base_row_index, STANDARD_VECTOR_SIZE);
            //        scan_state.row_index = segment.start + base_row_index;
            duckdb::ChimpScanPartial<double_t>(state, count, reinterpret_cast<uint8_t *>(dest));
        }
    }

}
