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

namespace LindormContest::storage {

// PageBuilder is used to build page
// Page is a data management unit, including:
// 1. Data Page: store encoded and compressed data
// 2. BloomFilter Page: store bloom filter of data
// 3. Ordinal Index Page: store ordinal index of data
// 4. Short Key Index Page: store short key index of data
// 5. Bitmap Index Page: store bitmap index of data
class PageBuilder {
public:
    PageBuilder() {}

    virtual ~PageBuilder() {}

    // Used by column writer to determine whether the current page is full.
    // Column writer depends on the result to decide whether to flush current page.
    virtual bool is_page_full() = 0;

    // Add a sequence of values to the page.
    // The number of values actually added will be returned through count, which may be less
    // than requested if the page is full.

    // check page if full before truly add, return ok when page is full so that column write
    // will switch to next page
    // vals size should be decided according to the page build type
    // TODO make sure vals is naturally-aligned to its type so that impls can use aligned load
    // instead of memcpy to copy values.
    virtual void add(const uint8_t* data, size_t* count) = 0;

    // Finish building the current page, return the encoded data.
    // This api should be followed by reset() before reusing the builder
    virtual OwnedSlice finish() = 0;

    // Get the dictionary page for dictionary encoding mode column.
    virtual void get_dictionary_page(OwnedSlice* dictionary_page) {
        throw std::runtime_error("get_dictionary_page not implemented");
    }

    // Reset the internal state of the page builder.
    //
    // Any data previously returned by finish may be invalidated by this call.
    virtual void reset() = 0;

    // Return the number of entries that have been added to the page.
    virtual size_t count() const = 0;

    // Return the total bytes of pageBuilder that have been added to the page.
    virtual UInt64 size() const = 0;

    // Return the first value in this page.
    // This method could only be called between finish() and reset().
    // void::NotFound if no values have been added.
    virtual void get_first_value(void* value) const = 0;

    // Return the last value in this page.
    // This method could only be called between finish() and reset().
    // void::NotFound if no values have been added.
    virtual void get_last_value(void* value) const = 0;
};

}