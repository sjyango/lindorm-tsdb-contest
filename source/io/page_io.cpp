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

#include "io/page_io.h"
#include "common/coding.h"

namespace LindormContest::io {

void PageIO::compress_page_body(CompressionEncoder* encoder, double min_space_saving,
                                const std::vector<Slice>& body, OwnedSlice* compressed_body) {
    size_t uncompressed_size = Slice::compute_total_size(body);
    if (encoder != nullptr && !encoder->exceed_max_compress_len(uncompressed_size)) {
        std::string buf;
        encoder->compress(body, uncompressed_size, &buf);
        double space_saving = 1.0 - static_cast<double>(buf.size()) / uncompressed_size;
        // return compressed body only when it saves more than min_space_saving
        if (space_saving > 0 && space_saving >= min_space_saving) {
            // shrink the buf to fit the len size to avoid taking
            // up the memory of the size MAX_COMPRESSED_SIZE
            *compressed_body = OwnedSlice(buf);
        }
    }
    // otherwise, do not compress
    OwnedSlice empty;
    *compressed_body = std::move(empty);
}

void PageIO::write_page(io::FileWriter* writer, const std::vector<Slice>& body,
                        const storage::PageFooter& footer, PagePointer* result) {
    std::string footer_buffer; // serialized footer + footer size
    footer.serialize(&footer_buffer);
    put_fixed32_le(&footer_buffer, static_cast<uint32_t>(footer_buffer.size()));

    std::vector<Slice> page = body;
    page.emplace_back(footer_buffer);

    uint64_t offset = writer->bytes_appended();
    writer->appendv(&page[0], page.size());

    result->_offset = offset;
    result->_size = writer->bytes_appended() - offset;
}

}