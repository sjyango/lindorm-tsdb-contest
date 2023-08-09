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
            : _fs(fs), _table_schema(table_schema) {}

    ~TableReader() = default;

    void init(PartialSchemaSPtr schema) {
        _schema = schema;
        std::vector<io::FileInfo> file_infos;
        _fs->list(_fs->root_path(), true, &file_infos);
        assert(file_infos.size() > 0);

        for (const auto& file_info : file_infos) {
            if (file_info._file_size <= 4 || file_info._file_name == "schema.txt" || file_info._file_name == "next_segment_id") {
                continue;
            }
            io::Path segment_path = _fs->root_path() / file_info._file_name;
            io::FileReaderSPtr file_reader = _fs->open_file(segment_path);
            size_t segment_id;
            std::sscanf(file_info._file_name.c_str(), "segment_%zd.dat", &segment_id);
            std::unique_ptr<SegmentReader> segment_reader = std::make_unique<SegmentReader>(segment_id, file_reader, _table_schema, _schema);
            _segment_readers.emplace(segment_id, std::move(segment_reader));
            _file_readers.emplace(segment_id, file_reader);
        }
    }

    void reset() {
        _schema.reset();
        _segment_readers.clear();
        _file_readers.clear();
    }

    // just for ut debug
    void scan_all(size_t* n, std::vector<Row>& results) {
        for (auto& item : _segment_readers) {
            std::unique_ptr<SegmentReader>& segment_reader = item.second;
            vectorized::Block block = _table_schema->create_block();
            segment_reader->seek_to_first();
            segment_reader->next_batch(n, &block);
            std::vector<Row> read_rows = block.to_rows();
            results.insert(results.end(), read_rows.begin(), read_rows.end());
        }
    }

    void handle_latest_query(const std::vector<Vin>& vins, std::vector<Row>& results) {
        for (const auto& vin : vins) {
            Row result_row;
            result_row.timestamp = -1;

            for (auto it = _segment_readers.rbegin(); it != _segment_readers.rend(); ++it) {
                auto result = it->second->handle_latest_query(vin);
                if (result.has_value()) {
                    if ((*result).timestamp > result_row.timestamp) {
                        std::memcpy(result_row.vin.vin, (*result).vin.vin, 17);
                        result_row.timestamp = (*result).timestamp;
                        result_row.columns = std::move((*result).columns);
                    }
                }
            }

            if (result_row.timestamp != -1) {
                results.emplace_back(result_row);
            }
        }
        INFO_LOG("handle latest query success, results size is %zu", vins.size())
    }

    void handle_time_range_query(Vin query_vin, size_t lower_bound_timestamp, size_t upper_bound_timestamp, std::vector<Row>& results) {
        std::vector<Row> table_rows;

        for (auto& [segment_id, segment_reader] : _segment_readers) {
            auto result = segment_reader->handle_time_range_query(query_vin, lower_bound_timestamp, upper_bound_timestamp);
            if (result.has_value()) {
                table_rows = std::move((*result).to_rows());
            }
        }

        results.insert(results.end(), table_rows.begin(), table_rows.end());
        INFO_LOG("handle time range query success, results size is %zu", table_rows.size())
    }

    // void handle_time_range_query(Vin query_vin, size_t lower_bound_timestamp, size_t upper_bound_timestamp, std::vector<Row>& results) {
    //     std::vector<std::vector<Row>> table_rows;
    //
    //     for (auto& [segment_id, segment_reader] : _segment_readers) {
    //         auto result = segment_reader->handle_time_range_query(query_vin, lower_bound_timestamp, upper_bound_timestamp);
    //         if (result.has_value()) {
    //             table_rows.push_back(std::move((*result).to_rows()));
    //         }
    //     }
    //
    //     _deduplication(table_rows, results);
    // }

private:
    void _deduplication(std::vector<std::vector<Row>>& table_rows, std::vector<Row>& results) {
        std::unordered_set<Row, RowHashFunc> deduplication_rows;

        for (auto it = table_rows.rbegin(); it != table_rows.rend(); ++it) {
            std::vector<Row>& segment_rows = *it;

            for (auto& row : segment_rows) {
                if (deduplication_rows.find(row) == deduplication_rows.end()) {
                    deduplication_rows.insert(std::move(row));
                }
            }
        }

        for (Row row : deduplication_rows) {
            results.push_back(std::move(row));
        }
    }

    struct RowHashFunc {
        size_t operator()(const Row& row) const {
            std::string vin_str(row.vin.vin, 17);
            return std::hash<std::string>()(vin_str) ^ std::hash<int64_t>()(row.timestamp);
        }
    };

    io::FileSystemSPtr _fs;
    PartialSchemaSPtr _schema;
    TableSchemaSPtr _table_schema;
    std::map<size_t, std::unique_ptr<SegmentReader>> _segment_readers;
    std::unordered_map<size_t, io::FileReaderSPtr> _file_readers;
};

}