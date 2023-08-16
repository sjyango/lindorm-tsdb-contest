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

#include <algorithm>
#include <unordered_map>

#include "Root.h"
#include "storage/partial_schema.h"
#include "storage/segment_reader.h"
#include "storage/segment_traits.h"
#include "struct/Requests.h"
#include "vec/blocks/block.h"
#include "io/io_utils.h"

namespace LindormContest::storage {

class TableReader {
public:
    TableReader(io::FileSystemSPtr fs, TableSchemaSPtr table_schema)
            : _fs(fs), _table_schema(table_schema) {
        init_segment_readers();
    }

    ~TableReader() = default;

    void init_segment_readers() {
        std::vector<io::FileInfo> file_infos;
        _fs->list(_fs->root_path(), true, &file_infos);

        for (const auto& file_info : file_infos) {
            if (file_info._file_size <= 4 ||
                file_info._file_name == "schema.txt" ||
                file_info._file_name == "next_segment_id" ||
                file_info._file_name == "latest_records.dat") {
                continue;
            }
            size_t segment_id;
            std::sscanf(file_info._file_name.c_str(), "segment_%zd.dat", &segment_id);
            if (_segment_readers.find(segment_id) != _segment_readers.end()) {
                continue;
            }
            io::Path segment_path = _fs->root_path() / file_info._file_name;
            io::FileReaderSPtr file_reader = _fs->open_file(segment_path);
            std::unique_ptr<SegmentReader> segment_reader = std::make_unique<SegmentReader>(file_reader, _table_schema);
            _segment_readers.emplace(segment_id, std::move(segment_reader));
            _file_readers.emplace(segment_id, file_reader);
            INFO_LOG("Load segment %zu success", segment_id)
        }
    }

    void handle_latest_query(const PartialSchemaSPtr& schema, const std::vector<RowPosition>& row_positions, std::vector<Row>& results) {
        for (const auto& row_position : row_positions) {
            assert(_segment_readers.find(row_position._segment_id) != _segment_readers.end());
            results.emplace_back(_segment_readers[row_position._segment_id]->handle_latest_query(schema, row_position._ordinal));
        }
    }

    void handle_time_range_query(const PartialSchemaSPtr& schema, const Vin& query_vin, int64_t lower_bound_timestamp, int64_t upper_bound_timestamp, std::vector<Row>& results) {
        for (auto& [segment_id, segment_reader] : _segment_readers) {
            auto result = segment_reader->handle_time_range_query(schema, query_vin, lower_bound_timestamp, upper_bound_timestamp);
            if (result.has_value()) {
                std::vector<Row> segment_rows = (*result).to_rows();
                results.insert(results.end(), segment_rows.begin(), segment_rows.end());
            }
        }
    }

private:
    io::FileSystemSPtr _fs;
    TableSchemaSPtr _table_schema;
    std::map<size_t, std::unique_ptr<SegmentReader>> _segment_readers;
    std::unordered_map<size_t, io::FileReaderSPtr> _file_readers;
};

}