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

#include <fstream>

#include "Root.h"

namespace LindormContest::io {

    static void stream_write_string_to_file(const Path& file_path, const std::string& buf) {
        std::ofstream output_file(file_path, std::ios::out | std::ios::binary);
        if (!output_file.is_open() || !output_file.good()) {
            throw std::runtime_error("open file failed");
        }
        output_file << buf;
        output_file.flush();
        output_file.close();
    }

    static void stream_read_string_from_file(const Path& file_path, std::string& buf) {
        std::ifstream input_file(file_path, std::ios::in | std::ios::binary);
        if (!input_file.is_open() || !input_file.good()) {
            throw std::runtime_error("open file failed");
        }
        input_file >> buf;
        input_file.close();
    }

    static void stream_read_string_from_file(const Path& file_path, uint32_t offset, uint32_t size, std::string& buf) {
        std::ifstream input_file(file_path, std::ios::in | std::ios::binary);
        if (!input_file.is_open() || !input_file.good()) {
            throw std::runtime_error("open file failed");
        }

        input_file.seekg(offset, std::ios::beg);
        buf.resize(size);
        input_file.read(buf.data(), size);
        input_file.close();
    }

}


// #include <cstring>
// #include <sys/mman.h>
// #include <fcntl.h>
// #include <unistd.h>

// static void mmap_write_string_to_file(const Path& file_path, const std::string& buf) {
//     int fd = open(file_path.c_str(), O_RDWR | O_CREAT);
//     if (fd == -1) {
//         throw std::runtime_error("open file failed");
//     }
//     size_t buf_size = buf.size();
//     if (ftruncate(fd, buf_size) == -1) {
//         throw std::runtime_error("set file size failed");
//     }
//     void* file_memory = mmap(nullptr, buf_size, PROT_WRITE, MAP_SHARED, fd, 0);
//     if (file_memory == MAP_FAILED) {
//         throw std::runtime_error("unable to map file to memory");
//     }
//     std::memcpy(file_memory, buf.c_str(), buf_size);
//     munmap(file_memory, buf_size);
//     close(fd);
// }

// static void mmap_read_string_from_file(const Path& file_path, std::string& buf) {
//     int fd = open(file_path.c_str(), O_RDONLY);
//     if (fd == -1) {
//         throw std::runtime_error("open file failed");
//     }
//     off_t file_size = lseek(fd, 0, SEEK_END);
//     void* file_memory = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
//     if (file_memory == MAP_FAILED) {
//         throw std::runtime_error("unable to map file to memory");
//     }
//     const char* file_content = static_cast<const char*>(file_memory);
//     buf.assign(file_content, file_size);
//     munmap(file_memory, file_size);
//     close(fd);
// }
//
// static void mmap_read_string_from_file(const Path& file_path, uint32_t offset, uint32_t size, std::string& buf) {
//     int fd = open(file_path.c_str(), O_RDONLY);
//     if (fd == -1) {
//         throw std::runtime_error("open file failed");
//     }
//     off_t file_size = lseek(fd, 0, SEEK_END);
//     if (offset >= file_size || size == 0 || offset + size > file_size) {
//         throw std::runtime_error("unable to map file to memory");
//     }
//     void* file_memory = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, offset);
//     if (file_memory == MAP_FAILED) {
//         perror("mmap error");
//         throw std::runtime_error("unable to map file to memory");
//     }
//     const char* file_content = static_cast<const char*>(file_memory);
//     buf.assign(file_content, size);
//     munmap(file_memory, size);
//     close(fd);
// }