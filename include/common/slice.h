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

namespace LindormContest {

class Slice {
public:
    char* _data;
    size_t _size;

    // Create an empty slice.
    Slice() : _data(nullptr), _size(0) {}

    Slice(char* s, size_t n) : _data(s), _size(n) {}

    Slice(const char* s, size_t n) : _data(const_cast<char*>(s)), _size(n) {}

    // Create a slice that refers to d[0,n-1].
    Slice(const UInt8* s, size_t n) : _data(const_cast<char*>(reinterpret_cast<const char*>(s))), _size(n) {}

    // Create a slice that refers to the contents of "s"
    Slice(const std::string& s) : _data(const_cast<char*>(s.data())), _size(s.size()) {}

    // Create a slice that refers to s[0,strlen(s)-1]
    Slice(const char* s) : _data(const_cast<char*>(s)), _size(strlen(s)) {}

    // Intentionally copyable.
    Slice(const Slice&) = default;
    Slice& operator=(const Slice&) = default;

    // Return a pointer to the beginning of the referenced data
    const char* data() const { return _data; }

    // Return the length (in bytes) of the referenced data
    size_t size() const { return _size; }

    // Return true iff the length of the referenced data is zero
    bool empty() const { return _size == 0; }

    // Return the ith byte in the referenced data.
    // REQUIRES: n < size()
    char operator[](size_t n) const {
        assert(n < size());
        return _data[n];
    }

    // Change this slice to refer to an empty array
    void clear() {
        _data = nullptr;
        _size = 0;
    }

    // Drop the first "n" bytes from this slice.
    void remove_prefix(size_t n) {
        assert(n <= size());
        _data += n;
        _size -= n;
    }

    // Return a string that contains the copy of the referenced data.
    String to_string() const {
        return String(reinterpret_cast<const char*>(_data), _size);
    }

    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare(const Slice& b) const {
        const size_t min_len = (_size < b._size) ? _size : b._size;
        int r = memcmp(_data, b._data, min_len);
        if (r == 0) {
            if (_size < b._size)
                r = -1;
            else if (_size > b._size)
                r = +1;
        }
        return r;
    }

    // Return true iff "x" is a prefix of "*this"
    bool starts_with(const Slice& x) const {
        return ((_size >= x._size) && (memcmp(_data, x._data, x._size) == 0));
    }

    static size_t compute_total_size(const std::vector<Slice>& slices) {
        size_t total_size = 0;
        for (auto& slice : slices) {
            total_size += slice._size;
        }
        return total_size;
    }
};

inline bool operator==(const Slice& x, const Slice& y) {
    return ((x.size() == y.size()) &&
            (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }

class OwnedSlice {
public:
    OwnedSlice() = default;
    
    OwnedSlice(const UInt8* _data, size_t size) {
        _owned_data = std::make_unique<UInt8[]>(size);
        _size = size;
        std::memcpy(_owned_data.get(), _data, _size);
    }

    OwnedSlice(String&& src) {
        _owned_data = std::make_unique<UInt8[]>(src.size());
        _size = src.size();
        std::memcpy(_owned_data.get(), src.data(), _size);
    }

    OwnedSlice(OwnedSlice&& src) {
        _owned_data = std::move(src._owned_data);
        _size = src._size;
    }

    OwnedSlice& operator=(OwnedSlice&& src) {
        if (this != &src) {
            std::swap(_owned_data, src._owned_data);
            std::swap(_size, src._size);
        }
        return *this;
    }

    ~OwnedSlice() = default;

    Slice slice() const {
        return Slice(_owned_data.get(), _size);
    }

    size_t size() const { return _size; }

private:
    std::unique_ptr<UInt8[]> _owned_data;
    size_t _size;
};

}