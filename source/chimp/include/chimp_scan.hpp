//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/chimp_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "chimp.hpp"
#include "algorithm/chimp_utils.hpp"
#include "common/limits.hpp"
#include "algorithm/flag_buffer.hpp"
#include "algorithm/leading_zero_buffer.hpp"
#include <memory>
#include "base.h"
#include "./function/compression_function.hpp"

namespace duckdb {

template <class CHIMP_TYPE>
struct ChimpGroupState {
public:
    void Init(uint8_t *data) {
        chimp_state.input.SetStream(data);
        Reset();
    }

    void Reset() {
        chimp_state.Reset();
        index = 0;
    }

    bool Started() const {
        return !!index;
    }

    // Assuming the group is completely full
    idx_t RemainingInGroup() const {
        return ChimpPrimitives::CHIMP_SEQUENCE_SIZE - index;
    }

    void Scan(CHIMP_TYPE *dest, idx_t count) {
        memcpy(dest, (void *)(values + index), count * sizeof(CHIMP_TYPE));
        index += count;
    }

    void LoadFlags(uint8_t *packed_data, idx_t group_size) {
        FlagBuffer<false> flag_buffer;
        flag_buffer.SetBuffer(packed_data);
        flags[0] = ChimpConstants::Flags::VALUE_IDENTICAL; // First value doesn't require a flag
        for (idx_t i = 0; i < group_size; i++) {
            flags[1 + i] = (ChimpConstants::Flags)flag_buffer.Extract();
        }
        max_flags_to_read = group_size;
        index = 0;
    }

    void LoadLeadingZeros(uint8_t *packed_data, idx_t leading_zero_block_size) {
        LeadingZeroBuffer<false> leading_zero_buffer;
        leading_zero_buffer.SetBuffer(packed_data);
        for (idx_t i = 0; i < leading_zero_block_size; i++) {
            leading_zeros[i] = ChimpConstants::Decompression::LEADING_REPRESENTATION[leading_zero_buffer.Extract()];
        }
        max_leading_zeros_to_read = leading_zero_block_size;
        leading_zero_index = 0;
    }

    idx_t CalculatePackedDataCount() const {
        idx_t count = 0;
        for (idx_t i = 0; i < max_flags_to_read; i++) {
            count += flags[1 + i] == ChimpConstants::Flags::TRAILING_EXCEEDS_THRESHOLD;
        }
        return count;
    }

    void LoadPackedData(uint16_t *packed_data, idx_t packed_data_block_count) {
        for (idx_t i = 0; i < packed_data_block_count; i++) {
            PackedDataUtils<CHIMP_TYPE>::Unpack(packed_data[i], unpacked_data_blocks[i]);
            if (unpacked_data_blocks[i].significant_bits == 0) {
                unpacked_data_blocks[i].significant_bits = 64;
            }
            unpacked_data_blocks[i].leading_zero =
                    ChimpConstants::Decompression::LEADING_REPRESENTATION[unpacked_data_blocks[i].leading_zero];
        }
        unpacked_index = 0;
        max_packed_data_to_read = packed_data_block_count;
    }

    void LoadValues(CHIMP_TYPE *result, idx_t count) {
        for (idx_t i = 0; i < count; i++) {
            result[i] = Chimp128Decompression<CHIMP_TYPE>::Load(flags[i], leading_zeros, leading_zero_index,
                                                                unpacked_data_blocks, unpacked_index, chimp_state);
        }
    }

public:
    uint32_t leading_zero_index;
    uint32_t unpacked_index;

    ChimpConstants::Flags flags[ChimpPrimitives::CHIMP_SEQUENCE_SIZE + 1];
    uint8_t leading_zeros[ChimpPrimitives::CHIMP_SEQUENCE_SIZE + 1];
    UnpackedData unpacked_data_blocks[ChimpPrimitives::CHIMP_SEQUENCE_SIZE];

    CHIMP_TYPE values[ChimpPrimitives::CHIMP_SEQUENCE_SIZE];

private:
    idx_t index;
    idx_t max_leading_zeros_to_read;
    idx_t max_flags_to_read;
    idx_t max_packed_data_to_read;
    Chimp128DecompressionState<CHIMP_TYPE> chimp_state;
};

template <class T>
struct ChimpScanState: public SegmentScanState {
public:
    using CHIMP_TYPE = typename ChimpType<T>::type;

    explicit ChimpScanState(uint8_t* compressData, size_t segCnt) : segment_count(segCnt) {
//        auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
        
        data = compressData;
        // ScanStates never exceed the boundaries of a Segment,
        // but are not guaranteed to start at the beginning of the Block
        auto start_of_data_segment = data + ChimpPrimitives::HEADER_SIZE;
        group_state.Init(start_of_data_segment);
        auto metadata_offset = Load<uint32_t>(data);
        metadata_ptr = data + metadata_offset;
    }

//    BufferHandle handle;
    uint8_t *metadata_ptr;
    idx_t total_value_count = 0;
    ChimpGroupState<CHIMP_TYPE> group_state;

//    ColumnSegment &segment;
    uint8_t* data;
    idx_t segment_count;

    idx_t LeftInGroup() const {
        return ChimpPrimitives::CHIMP_SEQUENCE_SIZE - (total_value_count % ChimpPrimitives::CHIMP_SEQUENCE_SIZE);
    }

    bool GroupFinished() const {
        return (total_value_count % ChimpPrimitives::CHIMP_SEQUENCE_SIZE) == 0;
    }

    template <class CHIMP_TYPE>
    void ScanGroup(CHIMP_TYPE *values, idx_t group_size) {
        assert(group_size <= ChimpPrimitives::CHIMP_SEQUENCE_SIZE);
        assert(group_size <= LeftInGroup());

        if (GroupFinished() && total_value_count < segment_count) {
            if (group_size == ChimpPrimitives::CHIMP_SEQUENCE_SIZE) {
                LoadGroup(values);
                total_value_count += group_size;
                return;
            } else {
                LoadGroup(group_state.values);
            }
        }
        group_state.Scan(values, group_size);
        total_value_count += group_size;
    }

    void LoadGroup(CHIMP_TYPE *value_buffer) {

        //! FIXME: If we change the order of this to flag -> leading_zero_blocks -> packed_data
        //! We can leave out the leading zero block count as well, because it can be derived from
        //! Extracting all the flags and counting the 3's

        // Load the offset indicating where a groups data starts
        metadata_ptr -= sizeof(uint32_t);
        auto data_byte_offset = Load<uint32_t>(metadata_ptr);
        assert(data_byte_offset < LindormContest::Storage::BLOCK_SIZE);
        //  Only used for point queries
        (void)data_byte_offset;

        // Load how many blocks of leading zero bits we have
        metadata_ptr -= sizeof(uint8_t);
        auto leading_zero_block_count = Load<uint8_t>(metadata_ptr);
        assert(leading_zero_block_count <= ChimpPrimitives::CHIMP_SEQUENCE_SIZE / 8);

        // Load the leading zero block count
        metadata_ptr -= 3 * leading_zero_block_count;
        const auto leading_zero_block_ptr = metadata_ptr;

        // Figure out how many flags there are
        assert(segment_count >= total_value_count);
        auto group_size = MinValue<idx_t>(segment_count - total_value_count, ChimpPrimitives::CHIMP_SEQUENCE_SIZE);
        // Reduce by one, because the first value of a group does not have a flag
        auto flag_count = group_size - 1;
        uint16_t flag_byte_count = (AlignValue<uint16_t, 4>(flag_count) / 4);

        // Load the flags
        metadata_ptr -= flag_byte_count;
        auto flags = metadata_ptr;
        group_state.LoadFlags(flags, flag_count);

        // Load the leading zero blocks
        group_state.LoadLeadingZeros(leading_zero_block_ptr, (uint32_t)leading_zero_block_count * 8);

        // Load packed data blocks
        auto packed_data_block_count = group_state.CalculatePackedDataCount();
        metadata_ptr -= packed_data_block_count * 2;
        if ((uint64_t)metadata_ptr & 1) {
            // Align on a two-byte boundary
            metadata_ptr--;
        }
        group_state.LoadPackedData((uint16_t *)metadata_ptr, packed_data_block_count);

        group_state.Reset();

        // Load all values for the group
        group_state.LoadValues(value_buffer, group_size);
    }

public:
    //! Skip the next 'skip_count' values, we don't store the values
    // TODO: use the metadata to determine if we can skip a group
    void Skip(ColumnSegment &segment, idx_t skip_count) {
        using INTERNAL_TYPE = typename ChimpType<T>::type;
        INTERNAL_TYPE buffer[ChimpPrimitives::CHIMP_SEQUENCE_SIZE];

        while (skip_count) {
            auto skip_size = MinValue(skip_count, LeftInGroup());
            ScanGroup<CHIMP_TYPE>(buffer, skip_size);
            skip_count -= skip_size;
        }
    }
};

//template <class T>
//unique_ptr<SegmentScanState> ChimpInitScan(ColumnSegment &segment) {
//    auto result = make_uniq_base<SegmentScanState, ChimpScanState<T>>(segment);
//    return result;
//}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void DecompressChimp(uint8_t* compress, size_t segCnt, uint8_t* recover){
    ChimpScanState<T> state = ChimpScanState<T>(compress,segCnt);
    for (idx_t base_row_index = 0; base_row_index < segCnt; base_row_index += 2048) {
        idx_t count = MinValue<idx_t>(segCnt - base_row_index, 2048);
//        scan_state.row_index = segment.start + base_row_index;
        ChimpScanPartial<T>(state, count, recover);
    }
}

template <class T>
void ChimpScanPartial(ChimpScanState<T> &scan_state, idx_t scan_count, uint8_t *result) {
    using INTERNAL_TYPE = typename ChimpType<T>::type;
    auto current_result_ptr = (INTERNAL_TYPE *)(result);

    idx_t scanned = 0;
    while (scanned < scan_count) {
        idx_t to_scan = MinValue(scan_count - scanned, scan_state.LeftInGroup());
        scan_state.template ScanGroup<INTERNAL_TYPE>(current_result_ptr + scanned, to_scan);
        scanned += to_scan;
    }
}

//template <class T>
//void ChimpSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
//    auto &scan_state = (ChimpScanState<T> &)*state.scan_state;
//    scan_state.Skip(segment, skip_count);
//}
//
//template <class T>
//void ChimpScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
//    ChimpScanPartial<T>(segment, state, scan_count, result, 0);
//}

} // namespace duckdb
