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

#include "Root.h"
#include "column_reader.h"
#include "io/io_utils.h"
#include "segment_traits.h"
#include "table_schema.h"
#include "partial_schema.h"
#include "rowwise_iterator.h"
#include "segment_reader.h"
#include "storage/indexs/short_key_index.h"

namespace LindormContest::storage {

class Segment;

using SegmentSPtr = std::shared_ptr<Segment>;

// A Segment is used to represent a segment in memory format. When segment is
// generated, it won't be modified, so this struct aimed to help read operation.
// It will prepare all ColumnReader to create ColumnIterator as needed.
// And user can create a RowwiseIterator through new_iterator function.
class Segment : public std::enable_shared_from_this<Segment> {
public:
    static void open(io::FileSystemSPtr fs, const std::string& path, uint32_t segment_id,
                       TableSchemaSPtr tablet_schema, std::shared_ptr<Segment>* result);

    Segment(uint32_t segment_id, TableSchemaSPtr tablet_schema);

    void load_short_key_index();

    void new_segment_reader(PartialSchemaSPtr schema, std::unique_ptr<RowwiseIterator>* iter);

    void new_column_reader(const TableColumn& table_column,
                             std::unique_ptr<ColumnReader>* iter);

private:
    friend class SegmentReader;

    void _open();
    void _parse_footer();
    void _create_column_readers();

    io::FileReaderSPtr _file_reader;
    uint32_t _segment_id;
    TableSchemaSPtr _table_schema;
    SegmentFooter _footer;
    std::unordered_map<int32_t, std::unique_ptr<ColumnReader>> _column_readers;
    std::unique_ptr<ShortKeyIndexReader> _short_key_index_reader;
}

}