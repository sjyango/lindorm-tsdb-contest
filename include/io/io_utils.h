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

}