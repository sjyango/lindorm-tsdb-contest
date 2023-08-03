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

#include "Root.h"
#include "io_utils.h"
#include "io/file_writer.h"
#include "io/file_reader.h"

namespace LindormContest::io {

class FileSystem : public std::enable_shared_from_this<FileSystem> {
public:
    static FileSystemSPtr create(Path path);

    FileSystem(Path root_path) : _root_path(root_path) {}

    ~FileSystem() = default;

    // The following are public interface.
    // And derived classes should implement all xxx_impl methods.
    FileWriterPtr create_file(const Path& file);

    FileReaderSPtr open_file(const Path& file) {
        FileDescription fd;
        fd._path = file.native();
        return open_file(fd);
    }

    FileReaderSPtr open_file(const FileDescription& fd);

    void create_directory(const Path& dir, bool failed_if_exists = false) const;

    void delete_file(const Path& file) const;

    void delete_directory(const Path& dir) const;

    int64_t file_size(const Path& file) const;

    bool exists(const Path& path) const;

    // void batch_delete(const std::vector<Path>& files);

    // void list(const Path& dir, bool only_file, std::vector<FileInfo>* files, bool* exists);

    // void rename(const Path& orig_name, const Path& new_name);

    // void rename_dir(const Path& orig_name, const Path& new_name);

    virtual Path absolute_path(const Path& path) const {
        if (path.is_absolute()) {
            return path;
        }
        return _root_path / path;
    }
    
    FileSystemSPtr getSPtr() {
        return shared_from_this();
    }

    const Path& root_path() const {
        return _root_path;
    }

private:
    Path _root_path;
};

}



