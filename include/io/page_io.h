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
#include "io/compression.h"
#include "io/file_writer.h"
#include "io/io_utils.h"
#include "storage/segment_traits.h"

namespace LindormContest::io {

class PageIO {
public:
    static void compress_page_body(CompressionUtil* compression_util, const Slice& body,
                                   double min_space_saving, OwnedSlice* compressed_body);

    static void write_page(io::FileWriter* writer, OwnedSlice&& body,
                           const storage::PageFooter& footer, PagePointer* result);

    static void compress_and_write_page(CompressionUtil* compression_util, io::FileWriter* writer, OwnedSlice&& body,
                                        double min_space_saving, const storage::PageFooter& footer, PagePointer* result);

    //     `result' holds the memory of page data,
    //     `body' points to page body,
    //     `footer' stores the page footer.
    static void read_and_decompress_page(CompressionUtil* compression_util,
                                         const PagePointer& page_pointer, FileReaderSPtr file_reader,
                                         Slice* body, storage::PageFooter& footer, OwnedSlice* result);
};

}