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

#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include <stdint.h>
#include <sys/uio.h>

#include "io/file_writer.h"

namespace LindormContest::io {

FileWriter::FileWriter(Path&& path, int fd, FileSystemSPtr fs)
        : _path(std::move(path)), _fs(fs), _fd(fd), _opened(true) {}

FileWriter::~FileWriter() {
    if (_opened) {
        close();
    }
    assert(!_opened || _closed);
}

void FileWriter::appendv(const Slice* data, size_t data_cnt) {
    assert(!_closed);
    _dirty = true;

    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested.
    size_t bytes_req = 0;
    struct iovec iov[data_cnt];

    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        bytes_req += result._size;
        iov[i] = {result._data, result._size};
    }

    size_t completed_iov = 0;
    size_t n_left = bytes_req;

    while (n_left > 0) {
        // Never request more than IOV_MAX in one request.
        size_t iov_count = std::min(data_cnt - completed_iov, static_cast<size_t>(IOV_MAX));
        ssize_t res;
        RETRY_ON_EINTR(res, ::writev(_fd, iov + completed_iov, iov_count));
        if (res < 0) {
            ERR_LOG("cannot write to path")
            throw std::runtime_error("cannot write to path");
        }
        if (res == n_left) {
            // All requested bytes were read. This is almost always the case.
            n_left = 0;
            break;
        }
        // Adjust iovec vector based on bytes read for the next request.
        ssize_t bytes_rem = res;
        for (size_t i = completed_iov; i < data_cnt; i++) {
            if (bytes_rem >= iov[i].iov_len) {
                // The full length of this iovec was written.
                completed_iov++;
                bytes_rem -= iov[i].iov_len;
            } else {
                // Partially wrote this result.
                // Adjust the iov_len and iov_base to write only the missing data.
                iov[i].iov_base = static_cast<uint8_t*>(iov[i].iov_base) + bytes_rem;
                iov[i].iov_len -= bytes_rem;
                break; // Don't need to adjust remaining iovec's.
            }
        }
        n_left -= res;
    }

    assert(0 == n_left);
    _bytes_appended += bytes_req;
}

void FileWriter::write_at(size_t offset, const Slice& data) {
    assert(!_closed);
    _dirty = true;

    size_t bytes_req = data._size;
    char* from = data._data;

    while (bytes_req != 0) {
        auto res = ::pwrite(_fd, from, bytes_req, offset);
        if (-1 == res && errno != EINTR) {
            ERR_LOG("cannot write to path")
            throw std::runtime_error("cannot write to path");
        }
        if (res > 0) {
            from += res;
            bytes_req -= res;
        }
    }
}

void FileWriter::finalize() const {
    assert(!_closed);
    if (_dirty) {
        int flags = SYNC_FILE_RANGE_WRITE;
        if (sync_file_range(_fd, 0, 0, flags) < 0) {
            ERR_LOG("cannot sync file")
            throw std::runtime_error("cannot sync file");
        }
    }
}

void FileWriter::close() {
    return _close(true);
}

void FileWriter::_close(bool sync) {
    if (_closed) {
        return;
    }
    _closed = true;
    if (sync && _dirty) {
        if (0 != ::fdatasync(_fd)) {
            ERR_LOG("cannot fdatasync")
            throw std::runtime_error("cannot fdatasync");
        }
        _sync_dir(_path.parent_path());
        _dirty = false;
    }
    if (0 != ::close(_fd)) {
        ERR_LOG("cannot close file")
        throw std::runtime_error("cannot close file");
    }
}

void FileWriter::_sync_dir(const Path& dirname) {
    int fd;
    RETRY_ON_EINTR(fd, ::open(dirname.c_str(), O_DIRECTORY | O_RDONLY));
    if (-1 == fd) {
        ERR_LOG("cannot open dir")
        throw std::runtime_error("cannot open dir");
    }
    if (0 != ::fdatasync(fd)) {
        ERR_LOG("cannot fdatasync dir")
        throw std::runtime_error("cannot fdatasync dir");
    }
    ::close(fd);
}

}