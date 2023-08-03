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

#include <filesystem>

#include "common/coding.h"

namespace LindormContest::io {

#define RETRY_ON_EINTR(err, expr)                                                              \
    do {                                                                                       \
        static_assert(std::is_signed<decltype(err)>::value, #err " must be a signed integer"); \
        (err) = (expr);                                                                        \
    } while ((err) == -1 && errno == EINTR)

using Path = std::filesystem::path;

inline Path operator/(Path&& lhs, const Path& rhs) {
    return std::move(lhs /= rhs);
}

class FileReader;
class FileWriter;

using FileReaderSPtr = std::shared_ptr<FileReader>;
using FileWriterPtr = std::unique_ptr<FileWriter>;

class FileSystem;

using FileSystemSPtr = std::shared_ptr<FileSystem>;

struct FileInfo {
    // only file name, no path
    std::string _file_name;
    int64_t _file_size;
    bool _is_file;
};

struct FileDescription {
    std::string _path;
    int64_t _start_offset;
    // length of the file in bytes.
    // -1 means unset.
    // If the file length is not set, the file length will be fetched from the file system.
    int64_t _file_size = -1;
    // modification time of this file.
    // 0 means unset.
    int64_t _mtime = 0;
};

struct PagePointer {
    UInt64 _offset;
    UInt32 _size;

    PagePointer() : _offset(0), _size(0) {}

    PagePointer(uint64_t offset, uint32_t size) : _offset(offset), _size(size) {}

    void reset() {
        _offset = 0;
        _size = 0;
    }

    void serialize(std::string* buf) const {
        put_fixed64_le(buf, _offset);
        put_fixed32_le(buf, _size);
    }

    void deserialize(const uint8_t*& data) {
        _offset = *reinterpret_cast<const uint64_t*>(data);
        _size = *reinterpret_cast<const uint32_t*>(data + sizeof(uint64_t));
        data += sizeof(uint64_t) + sizeof(uint32_t);
    }

    const UInt8* decode_from(const UInt8* data, const UInt8* limit) {
        data = decode_varint64_ptr(data, limit, &_offset);
        if (data == nullptr) {
            return nullptr;
        }
        return decode_varint32_ptr(data, limit, &_size);
    }

    bool decode_from(Slice* input) {
        bool result = get_varint64(input, &_offset);
        if (!result) {
            return false;
        }
        return get_varint32(input, &_size);
    }

    void encode_to(String* dst) const {
        put_varint64_varint32(dst, _offset, _size);
    }

    bool operator==(const PagePointer& other) const {
        return _offset == other._offset && _size == other._size;
    }

    bool operator!=(const PagePointer& other) const { return !(*this == other); }
};

}