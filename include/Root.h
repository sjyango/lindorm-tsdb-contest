//
// Don't modify this file, the evaluation program is compiled
// based on this header file.
//

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

#include <cassert>
#include <set>
#include <vector>
#include <cstring>
#include <string>
#include <utility>
#include <ostream>
#include <map>
#include <memory>
#include <cstdio>
#include <iostream>
#include <cmath>
#include <cstdint>

namespace LindormContest {

using UInt8 = uint8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

using Float32 = float;
using Float64 = double;

using String = std::string;

using rowid_t = uint32_t;
using ordinal_t = uint64_t;

using ColumnId = uint32_t;

#define ERR_LOG(str, ...) {                                  \
    fprintf(stderr, "%s:%d. [ERROR]: ", __FILE__, __LINE__); \
    fprintf(stderr, str, ##__VA_ARGS__);                     \
    fprintf(stderr, "\n");                                   \
}

#define INFO_LOG(str, ...) {                                 \
    fprintf(stdout, "%s:%d. [INFO]: ", __FILE__, __LINE__);  \
    fprintf(stdout, str, ##__VA_ARGS__);                     \
    fprintf(stdout, "\n");                                   \
}

}


// class SegmentReader1 {
// public:
//     SegmentReader1(SegmentSPtr segment_data, SchemaSPtr schema)
//             : _segment_data(segment_data), _schema(schema) {
//         _short_key_index_reader = std::make_unique<ShortKeyIndexReader>();
//         _short_key_index_reader->parse(_segment_data->_short_key_index_page.get());
//         for (const auto& col_id : _schema->column_ids()) {
//             _column_iters.emplace(col_id, std::make_shared<ColumnIterator>(
//                                                   _segment_data->at(col_id), _segment_data->_num_rows));
//         }
//     }
//
//     // void new_iterator(SchemaSPtr schema, std::unique_ptr<RowwiseIterator>* iter) {
//     //     assert(_short_key_index_page);
//     //     _short_key_index_reader->parse(_short_key_index_page.get());
//     //     iter->reset(new SegmentIterator(this->shared_from_this(), schema));
//     //     iter->get()->init();
//     // }
//
//     void new_iterator(SchemaSPtr schema, std::unique_ptr<RowwiseIterator>* iter) {
//         iter->reset(new SegmentIterator(this->shared_from_this(), schema));
//     }
//
//     uint32_t segment_id() const {
//         return _segment_data->_segment_id;
//     }
//
//     uint32_t num_rows() const {
//         return _segment_data->_num_rows;
//     }
//
//     std::shared_ptr<BaseColumnIterator> get_column_iterator(ColumnId col_id) {
//         return _column_iters[col_id];
//     }
//
//     const ShortKeyIndexReader* get_short_key_index() const {
//         return _short_key_index_reader.get();
//     }
//
// private:
//     SegmentSPtr _segment_data;
//     SchemaSPtr _schema;
//     std::unordered_map<ColumnId, std::shared_ptr<ColumnIterator>> _column_iters;
//     std::unique_ptr<ShortKeyIndexReader> _short_key_index_reader;
// };
