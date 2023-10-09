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

#include <unordered_map>
#include <shared_mutex>

#include "common/spinlock.h"
#include "common/time_range.h"
#include "storage/tsm_file.h"

namespace LindormContest {

    // multi thread safe
    class IndexManager {
    public:
        IndexManager() = default;

        ~IndexManager() = default;

        void query_indexes(uint16_t file_idx, const std::string& column_name, const TimeRange& file_tr,
                           std::vector<IndexEntry>& index_entries, std::vector<IndexRange>& ranges) {
            _index_entries[file_idx][column_name].get_index_entries_and_ranges(file_tr, index_entries, ranges);
        }

        void decode_from_file(const Path& vin_dir_path, SchemaSPtr schema) {
            for (const auto& entry: std::filesystem::directory_iterator(vin_dir_path)) {
                uint32_t file_size, index_offset;
                TsmFile::get_size_and_offset(entry.path(), &file_size, &index_offset);
                std::string buf;
                uint32_t index_size = file_size - index_offset - sizeof(uint32_t);
                io::stream_read_string_from_file(entry.path(), index_offset, index_size, buf);
                uint16_t file_idx = std::stoi(entry.path().filename().string());
                std::unordered_map<std::string, IndexBlock>& index_blocks = _index_entries[file_idx];
                const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.c_str());

                for (const auto &[column_name, column_type]: schema->columnTypeMap) {
                    IndexBlock index_block;
                    index_block.decode_from(p);
                    index_blocks.emplace(column_name, std::move(index_block));
                }
            }
        }

    private:
        std::unordered_map<std::string, IndexBlock> _index_entries[TSM_FILE_COUNT];
    };

    class GlobalIndexManager;

    using GlobalIndexManagerSPtr = std::shared_ptr<GlobalIndexManager>;

    class GlobalIndexManager {
    public:
        GlobalIndexManager() = default;

        ~GlobalIndexManager() = default;

        void query_indexes(uint16_t vin_num, uint16_t file_idx, const std::string& column_name,
                           const TimeRange& file_tr, std::vector<IndexEntry>& index_entries, std::vector<IndexRange>& ranges) {
            _index_managers[vin_num].query_indexes(file_idx, column_name, file_tr, index_entries, ranges);
        }

        void decode_from_file(const Path& root_path, SchemaSPtr schema) {
            for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
                Path vin_dir_path = root_path / "compaction" / std::to_string(vin_num);
                _index_managers[vin_num].decode_from_file(vin_dir_path, schema);
            }
        }

    private:
        IndexManager _index_managers[VIN_NUM_RANGE];
    };
}