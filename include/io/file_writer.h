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

#include <cstddef>

#include "Root.h"
#include "io/file_system.h"
#include "common/slice.h"

namespace LindormContest::io {

class FileWriter {
public:
    FileWriter(Path&& path, int fd, FileSystemSPtr fs);

    ~FileWriter();

    void close();

    void append(const Slice& data) {
        return appendv(&data, 1);
    }

    void appendv(const Slice* data, size_t data_cnt);

    void write_at(size_t offset, const Slice& data);

    void finalize() const;

    Path path() const {
        return _path;
    }

    size_t bytes_appended() const {
        return _bytes_appended;
    }

    FileSystemSPtr fs() const {
        return _fs;
    }

protected:
    void _close(bool sync);
    void _sync_dir(const Path& dirname);

    Path _path;
    size_t _bytes_appended = 0;
    FileSystemSPtr _fs;
    int _fd; // owned
    bool _closed = false;
    bool _opened = false;
    bool _dirty = false;
};

}