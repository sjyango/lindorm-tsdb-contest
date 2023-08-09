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

#include <unistd.h>

#include "io/file_reader.h"

namespace LindormContest::io {

FileReader::FileReader(Path&& path, size_t file_size, int fd, FileSystemSPtr fs)
        : _fd(fd), _path(std::move(path)), _file_size(file_size), _fs(fs) {}

FileReader::~FileReader() {
    close();
}

void FileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        int res = -1;
        res = ::close(_fd);
        if (-1 == res) {
            throw std::runtime_error("failed to close file");
        }
        _fd = -1;
    }
}

void FileReader::read_at(size_t offset, Slice result, size_t* bytes_read) const {
    assert(!closed());
    if (offset > _file_size) {
        std::runtime_error("offset exceeds file size");
    }
    size_t bytes_req = result._size;
    char* to = result._data;
    bytes_req = std::min(bytes_req, _file_size - offset);
    *bytes_read = 0;

    while (bytes_req != 0) {
        auto res = ::pread(_fd, to, bytes_req, offset);
        if (-1 == res && errno != EINTR) {
            throw std::runtime_error("cannot read from file");
        }
        if (res == 0) {
            throw std::runtime_error("cannot read from file: unexpected EOF");
        }
        if (res > 0) {
            to += res;
            offset += res;
            bytes_req -= res;
            *bytes_read += res;
        }
    }
}

}