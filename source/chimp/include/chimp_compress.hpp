//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/chimp_compress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "chimp.hpp"
#include "./function/compression_function.hpp"
#include "common/helper.hpp"
#include "common/limits.hpp"
#include "base.h"
#include <memory>

#include <functional>

namespace duckdb {

template <class T>
struct ChimpCompressionState : public CompressionState {
public:
    using CHIMP_TYPE = typename ChimpType<T>::type;
    
    explicit ChimpCompressionState(uint8_t* compressSpaceData) {
        CreateEmptySegment(compressSpaceData);
        baseData = compressSpaceData;
        // These buffers are recycled for every group, so they only have to be set once
        state.AssignLeadingZeroBuffer((uint8_t *)leading_zero_blocks);
        state.AssignFlagBuffer((uint8_t *)flags);
        state.AssignPackedDataBuffer((uint16_t *)packed_data_blocks);
    }

//    ColumnDataCheckpointer &checkpointer;
//    CompressionFunction &function;
//    unique_ptr<ColumnSegment> current_segment;
//    BufferHandle handle;
    uint8_t *baseData;
    idx_t group_idx = 0;
    uint8_t flags[ChimpPrimitives::CHIMP_SEQUENCE_SIZE / 4];
    uint8_t leading_zero_blocks[ChimpPrimitives::LEADING_ZERO_BLOCK_BUFFERSIZE];
    uint16_t packed_data_blocks[ChimpPrimitives::CHIMP_SEQUENCE_SIZE];

    // Ptr to next free spot in segment;
    uint8_t* segment_data;
    uint8_t* metadata_ptr;
    uint32_t next_group_byte_index_start = ChimpPrimitives::HEADER_SIZE;
    // The total size of metadata in the current segment
    idx_t metadata_byte_size = 0;

    ChimpState<T, false> state;
    
    // new value
    size_t current_segment_count{0};

public:
    idx_t RequiredSpace() const {
        idx_t required_space = ChimpPrimitives::MAX_BYTES_PER_VALUE;
        // Any value could be the last,
        // so the cost of flushing metadata should be factored into the cost

        // byte offset of data
        required_space += sizeof(byte_index_t);
        // amount of leading zero blocks
        required_space += sizeof(uint8_t);
        // first leading zero block
        required_space += 3;
        // amount of flag bytes
        required_space += sizeof(uint8_t);
        // first flag byte
        required_space += 1;
        return required_space;
    }

    // How many bytes the data occupies for the current segment
    idx_t UsedSpace() const {
        return state.chimp.output.BytesWritten();
    }

    idx_t RemainingSpace() const {
        return metadata_ptr - (baseData + UsedSpace());
    }

    idx_t CurrentGroupMetadataSize() const {
        idx_t metadata_size = 0;

        metadata_size += 3 * state.chimp.leading_zero_buffer.BlockCount();
        metadata_size += state.chimp.flag_buffer.BytesUsed();
        metadata_size += 2 * state.chimp.packed_data_buffer.index;
        return metadata_size;
    }

    // The current segment has enough space to fit this new value
    bool HasEnoughSpace() {
        if (baseData + AlignValue(ChimpPrimitives::HEADER_SIZE + UsedSpace() + RequiredSpace()) >=
            (metadata_ptr - CurrentGroupMetadataSize())) {
            return false;
        }
        return true;
    }

    void CreateEmptySegment(uint8_t *data) {
        group_idx = 0;
        metadata_byte_size = 0;
        next_group_byte_index_start = ChimpPrimitives::HEADER_SIZE;
        

        segment_data = data + ChimpPrimitives::HEADER_SIZE;
        metadata_ptr = data + LindormContest::Storage::BLOCK_SIZE;
        state.AssignDataBuffer(segment_data);
        state.chimp.Reset();
    }

    void Append(uint8_t * vdata, idx_t count) {
        auto data = reinterpret_cast<CHIMP_TYPE*>(vdata);

        for (idx_t i = 0; i < count; i++) {
            WriteValue(data[i], true);
        }
    }

    void WriteValue(CHIMP_TYPE value, bool is_valid) {
        if (!HasEnoughSpace()) {
            // Segment is full
            assert(false);
//            auto row_start = current_segment->start + current_segment->count;
            FlushSegment();
            CreateEmptySegment(nullptr);
        }
        current_segment_count++;

        Chimp128Compression<CHIMP_TYPE, false>::Store(value, state.chimp);
        group_idx++;
        if (group_idx == ChimpPrimitives::CHIMP_SEQUENCE_SIZE) {
            FlushGroup();
        }
    }

    void FlushGroup() {
        // Has to be called first to flush the last values in the LeadingZeroBuffer
        state.chimp.Flush();

        metadata_ptr -= sizeof(byte_index_t);
        metadata_byte_size += sizeof(byte_index_t);
        // Store where this groups data starts, relative to the start of the segment
        Store<byte_index_t>(next_group_byte_index_start, metadata_ptr);
        next_group_byte_index_start = UsedSpace();

        const uint8_t leading_zero_block_count = state.chimp.leading_zero_buffer.BlockCount();
        // Every 8 values are packed in one block
        assert(leading_zero_block_count <= ChimpPrimitives::CHIMP_SEQUENCE_SIZE / 8);
        metadata_ptr -= sizeof(uint8_t);
        metadata_byte_size += sizeof(uint8_t);
        // Store how many leading zero blocks there are
        Store<uint8_t>(leading_zero_block_count, metadata_ptr);

        const uint64_t bytes_used_by_leading_zero_blocks = 3 * leading_zero_block_count;
        metadata_ptr -= bytes_used_by_leading_zero_blocks;
        metadata_byte_size += bytes_used_by_leading_zero_blocks;
        // Store the leading zeros (8 per 3 bytes) for this group
        memcpy((void *)metadata_ptr, (void *)leading_zero_blocks, bytes_used_by_leading_zero_blocks);

        //! This is max 1024, because it's the amount of flags there are, not the amount of bytes that takes up
        const uint16_t flag_bytes = state.chimp.flag_buffer.BytesUsed();

        metadata_ptr -= flag_bytes;
        metadata_byte_size += flag_bytes;
        // Store the flags (4 per byte) for this group
        memcpy((void *)metadata_ptr, (void *)flags, flag_bytes);

        // Store the packed data blocks (2 bytes each)
        // We dont need to store an extra count for this,
        // as the count can be derived from unpacking the flags and counting the '1' flags

        // FIXME: this does stop us from skipping groups with point queries,
        // because the metadata has a variable size, and we have to extract all flags + iterate them to know this size
        const uint16_t packed_data_blocks_count = state.chimp.packed_data_buffer.index;
        metadata_ptr -= packed_data_blocks_count * 2;
        metadata_byte_size += packed_data_blocks_count * 2;
        if ((uint64_t)metadata_ptr & 1) {
            // Align on a two-byte boundary
            metadata_ptr--;
            metadata_byte_size++;
        }
        memcpy((void *)metadata_ptr, (void *)packed_data_blocks, packed_data_blocks_count * sizeof(uint16_t));

        state.chimp.Reset();
        group_idx = 0;
    }

    // FIXME: only do this if the wasted space meets a certain threshold (>= 20%)
    size_t FlushSegment() {
        if (group_idx) {
            // Only call this when the group actually has data that needs to be flushed
            FlushGroup();
        }
        state.chimp.output.Flush();
//        auto &checkpoint_state = checkpointer.GetCheckpointState();
        auto dataptr = baseData;

        // Compact the segment by moving the metadata next to the data.
        idx_t bytes_used_by_data = ChimpPrimitives::HEADER_SIZE + UsedSpace();
        idx_t metadata_offset = AlignValue(bytes_used_by_data);
        // Verify that the metadata_ptr does not cross this threshold
        assert(dataptr + metadata_offset <= metadata_ptr);
        idx_t metadata_size = dataptr + LindormContest::Storage::BLOCK_SIZE - metadata_ptr;
        idx_t total_segment_size = metadata_offset + metadata_size;
        memmove(dataptr + metadata_offset, metadata_ptr, metadata_size);
        //  Store the offset of the metadata of the first group (which is at the highest address).
        Store<uint32_t>(metadata_offset + metadata_size, dataptr);
//        checkpoint_state.FlushSegment(std::move(current_segment), total_segment_size);
        return total_segment_size;
    }

    size_t Finalize() {
        return FlushSegment();
    }

    size_t GetSegCount(){
        return current_segment_count;
    }
};

// Compression Functions
template<class T>
size_t ChimpCompress(uint8_t* srcData, idx_t count, uint8_t* destData, size_t& compressSize){
    ChimpCompressionState<T> state = ChimpCompressionState<T>(destData);
    state.Append(srcData, count);
    compressSize = state.Finalize();
    return state.GetSegCount();
}

} // namespace duckdb
