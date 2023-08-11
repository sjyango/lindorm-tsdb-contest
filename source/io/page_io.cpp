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

void PageIO::compress_page_body(CompressionUtil* compression_util, const Slice& body, double min_space_saving, OwnedSlice* compressed_body) {
    size_t uncompressed_size = body.size();
    if (compression_util != nullptr) {
        std::string buf;
        compression_util->compress(body, uncompressed_size, &buf);
        double space_saving = 1.0 - static_cast<double>(buf.size()) / uncompressed_size;
        // return compressed body only when it saves more than min_space_saving
        if (space_saving > 0 && space_saving >= min_space_saving) {
            // shrink the buf to fit the len size to avoid taking
            // up the memory of the size MAX_COMPRESSED_SIZE
            *compressed_body = OwnedSlice(buf);
        }
    }
    // otherwise, do not compress
    *compressed_body = OwnedSlice();
}

void PageIO::write_page(FileWriter* writer, OwnedSlice&& body, const storage::PageFooter& footer, PagePointer* result) {
    std::string footer_buffer; // serialize footer content + footer size
    footer.serialize(&footer_buffer);
    put_fixed32_le(&footer_buffer, static_cast<uint32_t>(footer_buffer.size()));

    std::vector<Slice> data;
    data.emplace_back(body.slice());
    data.emplace_back(footer_buffer);

    uint64_t offset = writer->bytes_appended();
    writer->appendv(data.data(), data.size());
    result->_offset = offset;
    result->_size = writer->bytes_appended() - offset;
}

void PageIO::compress_and_write_page(CompressionUtil* compression_util, FileWriter* writer,
                                     OwnedSlice&& body, double min_space_saving,
                                     const storage::PageFooter& footer, PagePointer* result) {
    assert(footer._uncompressed_size == body.size());
    OwnedSlice compressed_body;
    compress_page_body(compression_util, body.slice(), min_space_saving, &compressed_body);
    if (compressed_body.size() == 0) { // uncompressed
        return write_page(writer, std::move(body), footer, result);
    }
    return write_page(writer, std::move(compressed_body), footer, result);
}

void PageIO::read_and_decompress_page(CompressionUtil* compression_util, const PagePointer& page_pointer,
                                      FileReaderSPtr file_reader, Slice* body, storage::PageFooter& footer,
                                      OwnedSlice* result) {
    const uint32_t page_size = page_pointer._size;
    const uint64_t page_offset = page_pointer._offset;
    if (page_size < 8) {
        throw std::runtime_error("Bad page: too small size");
    }
    // std::unique_ptr<storage::DataPage> page = std::make_unique<storage::DataPage>(page_size);
    std::unique_ptr<OwnedSlice> page = std::make_unique<OwnedSlice>(page_size);
    Slice page_slice = page->slice();
    size_t bytes_read = 0;
    file_reader->read_at(page_offset, page_slice, &bytes_read);
    assert(bytes_read == page_size);

    // deserialize footer content + footer size
    uint32_t footer_size = decode_fixed32_le(reinterpret_cast<const uint8_t*>(page_slice._data + page_slice._size - 4));
    const uint8_t* footer_start = reinterpret_cast<const uint8_t*>(page_slice._data + page_slice._size - 4 - footer_size);
    footer.deserialize(footer_start);
    uint32_t body_size = page_slice._size - 4 - footer_size;

    if (body_size != footer._uncompressed_size) { // need decompress body
        if (compression_util == nullptr) {
            throw std::runtime_error("Bad page: page is compressed but decoder is nullptr");
        }

        // std::unique_ptr<storage::DataPage> decompressed_page =
        //         std::make_unique<storage::DataPage>(footer._uncompressed_size + footer_size + 4);
        std::unique_ptr<OwnedSlice> decompressed_page =
                std::make_unique<OwnedSlice>(footer._uncompressed_size + footer_size + 4);

        // decompress page body
        Slice compressed_body(page_slice._data, body_size);
        Slice decompressed_body(decompressed_page->data(), footer._uncompressed_size);
        compression_util->decompress(compressed_body, &decompressed_body);

        if (decompressed_body._size != footer._uncompressed_size) {
            throw std::runtime_error("Bad page: record uncompressed size != real decompressed size");
        }

        // copy footer and footer size
        std::memcpy(decompressed_body._data + decompressed_body._size, page_slice._data + body_size,
                    footer_size + 4);
        // free memory of compressed page
        page = std::move(decompressed_page);
        page_slice = Slice(page->data(), footer._uncompressed_size + footer_size + 4);
    }

    *body = Slice(page_slice._data, page_slice._size - 4 - footer_size);
    *result = std::move(*page);
}

}