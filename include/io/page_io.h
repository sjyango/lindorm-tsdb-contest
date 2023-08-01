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
#include "compression.h"
#include "file_writer.h"
#include "io/io_utils.h"

namespace LindormContest::io {

class PageIO {
public:
    // Compress `body' using `codec' into `compressed_body'.
    // The size of returned `compressed_body' is 0 when the body is not compressed, this
    // could happen when `codec' is null or space saving is less than `min_space_saving'.
    static void compress_page_body(CompressionEncoder* codec, double min_space_saving,
                                     const std::vector<Slice>& body, OwnedSlice* compressed_body);
    
    // Encode page from `body' and `footer' and write to `file'.
    // `body' could be either uncompressed or compressed.
    // On success, the file pointer to the written page is stored in `result'.
    static void write_page(io::FileWriter* writer, const std::vector<Slice>& body,
                             const PageFooter& footer, PagePointer* result);

    // Convenient function to compress page body and write page in one go.
    static void compress_and_write_page(BlockCompressionCodec* codec, double min_space_saving,
                                          io::FileWriter* writer, const std::vector<Slice>& body,
                                          const PageFooterPB& footer, PagePointer* result) {
        DCHECK_EQ(footer.uncompressed_size(), Slice::compute_total_size(body));
        OwnedSlice compressed_body;
        RETURN_IF_ERROR(compress_page_body(codec, min_space_saving, body, &compressed_body));
        if (compressed_body.slice().empty()) { // uncompressed
            return write_page(writer, body, footer, result);
        }
        return write_page(writer, {compressed_body.slice()}, footer, result);
    }

    // Read and parse a page according to `opts'.
    // On success
    //     `handle' holds the memory of page data,
    //     `body' points to page body,
    //     `footer' stores the page footer.
    static void read_and_decompress_page(const PageReadOptions& opts, PageHandle* handle,
                                           Slice* body, PageFooterPB* footer);
};

}