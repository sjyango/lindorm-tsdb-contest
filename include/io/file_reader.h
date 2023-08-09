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

#include <atomic>

#include "Root.h"
#include "io/file_system.h"
#include "common/slice.h"

namespace LindormContest::io {

class FileReader {
public:
    FileReader(Path&& path, size_t file_size, int fd, FileSystemSPtr fs);
    
    ~FileReader();

    void close();

    void read_at(size_t offset, Slice result, size_t* bytes_read) const;

    Path path() const {
        return _path;
    }

    size_t size() const {
        return _file_size;
    }

    bool closed() const {
        return _closed.load(std::memory_order_acquire);
    }

    FileSystemSPtr fs() const {
        return _fs;
    }

private:
    int _fd = -1; // owned
    Path _path;
    size_t _file_size;
    std::atomic<bool> _closed = false;
    FileSystemSPtr _fs;
};

}