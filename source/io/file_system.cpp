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

#include "io/file_system.h"
#include "io/io_utils.h"

namespace LindormContest::io {

FileSystemSPtr FileSystem::create(Path path) {
    return std::make_shared<FileSystem>(path);
}

FileWriterPtr FileSystem::create_file(const Path& file) {
    Path abs_path = absolute_path(file);
    int fd = ::open(abs_path.c_str(), O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, 0666);
    if (-1 == fd) {
        ERR_LOG("failed to open file")
        throw std::runtime_error("failed to open file");
    }
    return std::move(std::make_unique<FileWriter>(std::move(abs_path), fd, shared_from_this()));
}

FileReaderSPtr FileSystem::open_file(const FileDescription& file_desc) {
    Path abs_path = absolute_path(file_desc._path);
    int64_t fsize = file_desc._file_size;
    if (fsize <= 0) {
        fsize = file_size(abs_path);
    }
    int fd = -1;
    RETRY_ON_EINTR(fd, open(abs_path.c_str(), O_RDONLY));
    if (fd < 0) {
        ERR_LOG("failed to open file")
        throw std::runtime_error("failed to open file");
    }
    return std::make_shared<FileReader>(std::move(abs_path), fsize, fd, shared_from_this());

}

void FileSystem::create_directory(const Path& dir, bool failed_if_exists) const {
    Path abs_path = absolute_path(dir);
    if (failed_if_exists) {
        if (exists(abs_path)) {
            ERR_LOG("failed to create file, already exists")
            throw std::runtime_error("failed to create file, already exists");
        }
    }
    std::error_code ec;
    std::filesystem::create_directories(abs_path, ec);
    if (ec) {
        ERR_LOG("failed to create directory")
        throw std::runtime_error("failed to create directory");
    }
}

bool FileSystem::exists(const Path& path) const {
    Path abs_path = absolute_path(path);
    std::error_code ec;
    bool res = std::filesystem::exists(abs_path, ec);
    if (ec) {
        ERR_LOG("failed to check file exists")
        throw std::runtime_error("failed to check file exists");
    }
    return res;
}

void FileSystem::list(const Path& dir, bool only_file, std::vector<FileInfo>* files) const {
    Path abs_path = absolute_path(dir);
    assert(exists(dir));
    std::error_code ec;

    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        if (only_file && !entry.is_regular_file()) {
            continue;
        }
        FileInfo file_info;
        file_info._file_name = entry.path().filename();
        file_info._is_file = entry.is_regular_file(ec);
        if (ec) {
            break;
        }
        if (file_info._is_file) {
            file_info._file_size = entry.file_size(ec);
            if (ec) {
                break;
            }
        }
        files->push_back(std::move(file_info));
    }
    if (ec) {
        ERR_LOG("failed to list directory")
        throw std::runtime_error("failed to list directory");
    }
}

void FileSystem::delete_file(const Path& file) const {
    Path abs_path = absolute_path(file);
    if (!exists(abs_path)) {
        return;
    }
    if (!std::filesystem::is_regular_file(abs_path)) {
        ERR_LOG("failed to delete file, because not a file")
        throw std::runtime_error("failed to delete file, because not a file");
    }
    std::error_code ec;
    std::filesystem::remove(abs_path, ec);
    if (ec) {
        ERR_LOG("failed to delete file")
        throw std::runtime_error("failed to delete file");
    }
}

void FileSystem::delete_directory(const Path& dir) const {
    Path abs_path = absolute_path(dir);
    if (!exists(abs_path)) {
        return;
    }
    if (!std::filesystem::is_directory(abs_path)) {
        ERR_LOG("failed to delete directory, because not a directory")
        throw std::runtime_error("failed to delete directory, because not a directory");
    }
    std::error_code ec;
    std::filesystem::remove_all(abs_path, ec);
    if (ec) {
        ERR_LOG("failed to delete directory")
        throw std::runtime_error("failed to delete directory");
    }
}

int64_t FileSystem::file_size(const Path& file) const {
    Path abs_path = absolute_path(file);
    std::error_code ec;
    int64_t file_size = std::filesystem::file_size(abs_path, ec);
    if (ec) {
        ERR_LOG("failed to get file size")
        throw std::runtime_error("failed to get file size");
    }
    return file_size;
}

}