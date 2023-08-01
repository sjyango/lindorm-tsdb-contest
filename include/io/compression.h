/*
* Copyright Alibaba Group Holding Ltd.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#pragma once

#include "Root.h"
#include "common/slice.h"
#include "storage/segment_traits.h"

namespace LindormContest::io {

class CompressionEncoder {
public:
    virtual ~CompressionEncoder() = default;
    
    virtual void init() {
        throw std::runtime_error("init doesn't implement");
    }

    // This function will compress input data into output.
    // output should be preallocated, and its capacity must be large enough
    // for compressed input, which can be get through max_compressed_len function.
    // Size of compressed data will be set in output's size.
    virtual void compress(const Slice& input, std::string* output) {
        throw std::runtime_error("compress doesn't implement");
    }

    // Default implementation will merge input list into a big buffer and call
    // compress(Slice) to finish compression. If compression type support digesting
    // slice one by one, it should reimplement this function.
    virtual void compress(const std::vector<Slice>& input, size_t uncompressed_size,
                            std::string* output) {
        throw std::runtime_error("compress doesn't implement");
    }

    // Decompress input data into output, output's capacity should be large enough
    // for decompressed data.
    // Size of decompressed data will be set in output's size.
    virtual void decompress(const Slice& input, Slice* output) {
        throw std::runtime_error("decompress doesn't implement");
    }

    // Returns an upper bound on the max compressed length.
    virtual size_t max_compressed_len(size_t len) {
        throw std::runtime_error("max_compressed_len doesn't implement");
    }

    virtual bool exceed_max_compress_len(size_t uncompressed_size) {
        if (uncompressed_size > std::numeric_limits<int32_t>::max()) {
            return true;
        }
        return false;
    }
};


inline void get_compression_encoder(storage::CompressionType type, CompressionEncoder** compression_encoder) {
    switch (type) {
    case storage::CompressionType::NO_COMPRESSION:
        *compression_encoder = nullptr;
        break;
    default:
        return;
    }
}

}