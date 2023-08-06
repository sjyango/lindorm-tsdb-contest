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
#include "partial_schema.h"
#include "rowwise_iterator.h"
#include "segment_reader.h"
#include "segment_traits.h"
#include "struct/Requests.h"
#include "vec/blocks/block.h"
#include "io/io_utils.h"

namespace LindormContest::storage {

class TableReader {
public:
    TableReader(io::FileSystemSPtr fs, TableSchemaSPtr table_schema)
            : _fs(fs), _table_schema(table_schema), _inited(false) {}

    ~TableReader() = default;

    void init(PartialSchemaSPtr schema) {
        _schema = schema;
        std::vector<io::FileInfo> file_infos;
        _fs->list(_fs->root_path(), true, &file_infos);
        assert(file_infos.size() > 0);

        for (const auto& file_info : file_infos) {
            if (file_info._file_size <= 4) {
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

        _inited = true;
    }

    void reset() {
        _schema = nullptr;
        _segment_readers.clear();
        _file_readers.clear();
        _inited = false;
    }

    // just for ut debug
    void scan_all(size_t* n, std::vector<Row>& results) {
        for (auto& item : _segment_readers) {
            std::unique_ptr<SegmentReader>& segment_reader = item.second;
            vectorized::Block block = _table_schema->create_block();
            segment_reader->seek_to_first();
            segment_reader->next_batch(n, &block);
            std::vector<Row> read_rows = block.to_rows(0, block.rows());
            results.insert(results.end(), read_rows.begin(), read_rows.end());
        }
    }

private:
    bool _inited = false;
    io::FileSystemSPtr _fs;
    PartialSchemaSPtr _schema;
    TableSchemaSPtr _table_schema;
    std::unordered_map<size_t, std::unique_ptr<SegmentReader>> _segment_readers;
    std::unordered_map<size_t, io::FileReaderSPtr> _file_readers;
};

}