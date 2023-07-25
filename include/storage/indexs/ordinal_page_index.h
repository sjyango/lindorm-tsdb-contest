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
#include "storage/index_page_builder.h"
#include "storage/segment_traits.h"

namespace LindormContest::storage {

// Ordinal index is implemented by one IndexPage that stores
// the `first value ordinal` and `file pointer` for each data page.
class OrdinalIndexWriter {
public:
    OrdinalIndexWriter() : _page_builder(new IndexPageBuilder(0, true)) {}

    void append_entry(ordinal_t ordinal, const PagePointer& page_pointer) {
        String key;
        put_fixed64_le(&key, ordinal);
        _page_builder->add(key, page_pointer);
        _last_page_pointer = page_pointer;
    }

    uint64_t size() { return _page_builder->size(); }

    // Status finish(io::FileWriter* file_writer, ColumnIndexMetaPB* meta) {
    //
    // }

private:
    std::unique_ptr<IndexPageBuilder> _page_builder;
    PagePointer _last_page_pointer;
};

}