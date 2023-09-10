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

#include <variant>

#include "Root.h"
#include "struct/Schema.h"
#include "common/coding.h"
#include "common/thread_pool.h"
#include "storage/memmap.h"
#include "storage/tsm_file.h"
#include "compression/compressor.h"

namespace LindormContest {

    class TsmWriter {
    public:
        TsmWriter(ThreadPoolSPtr flush_pool, const Path& flush_dir_path,
                  std::string vin_str, SchemaSPtr schema);

        ~TsmWriter();

        void append(const Row &row);

        void flush_mem_map_async();

        void reset_mem_map();

        static void flush_mem_map(MemMap *mem_map, SchemaSPtr schema, Path tsm_file_path);

    private:
        std::mutex _mutex;
        ThreadPoolSPtr _flush_pool;
        Path _flush_dir_path;
        SchemaSPtr _schema;
        std::unique_ptr<MemMap> _mem_map;
        uint16_t _flush_nums;
    };
}


// void encode_to(std::string* buf) const {
//     std::string uncompress_ts_data;
//     std::string uncompress_val_data;
//     uncompress_ts_data.append(reinterpret_cast<const char*>(_tss.data()), _count * sizeof(int64_t));
//
//     for (const auto &val: _column_values) {
//         uncompress_val_data.append(val.columnData, val.getRawDataSize());
//     }
//
//     uint32_t uncompress_ts_size = uncompress_ts_data.size();
//     uint32_t uncompress_val_size = uncompress_val_data.size();
//
//     std::unique_ptr<char[]> compress_ts_data = std::make_unique<char[]>(uncompress_ts_size * 1.2);
//     std::unique_ptr<char[]> compress_val_data = std::make_unique<char[]>(uncompress_val_size * 1.2);
//
//     uint32_t compress_ts_size = compression::compress_int64(uncompress_ts_data.c_str(),uncompress_ts_size, compress_ts_data.get());
//     uint32_t compress_val_size;
//
//     switch (_type) {
//         case COLUMN_TYPE_INTEGER:
//             compress_val_size = compression::compress_int32(uncompress_val_data.c_str(), uncompress_val_size, compress_val_data.get());
//             break;
//         case COLUMN_TYPE_DOUBLE_FLOAT:
//             compress_val_size = compression::compress_float(uncompress_val_data.c_str(), uncompress_val_size, compress_val_data.get());
//             break;
//         case COLUMN_TYPE_STRING:
//             compress_val_size = compression::compress_string(uncompress_val_data.c_str(), uncompress_val_size, compress_val_data.get());
//             break;
//         default:
//             throw std::runtime_error("invalid column type");
//     }
//
//     put_fixed(buf, uncompress_ts_size);
//     put_fixed(buf, compress_ts_size);
//     buf->append(compress_ts_data.get(), compress_ts_size);
//     put_fixed(buf, uncompress_val_size);
//     put_fixed(buf, compress_val_size);
//     buf->append(compress_val_data.get(), compress_val_size);
// }
//
// void decode_from(const uint8_t*& buf) {
//     _count = decode_fixed<uint32_t>(buf);
//     _type = (ColumnType) decode_fixed<uint8_t>(buf);
//
//     // decode ts
//     uint32_t uncompress_ts_size = decode_fixed<uint32_t>(buf);
//     assert(uncompress_ts_size == _count * sizeof(int64_t));
//     uint32_t compress_ts_size = decode_fixed<uint32_t>(buf);
//     std::unique_ptr<char[]> uncompress_ts_data = std::make_unique<char[]>(uncompress_ts_size * 1.2);
//     auto start_ptr = compression::decompress_int64(reinterpret_cast<const char*>(buf), compress_ts_size,
//                                                    uncompress_ts_data.get(), uncompress_ts_size);
//     _tss.resize(_count);
//     std::memcpy(_tss.data(), start_ptr, uncompress_ts_size);
//     buf += compress_ts_size;
//
//     // decode value
//     uint32_t uncompress_val_size = decode_fixed<uint32_t>(buf);
//     uint32_t compress_val_size = decode_fixed<uint32_t>(buf);
//     std::unique_ptr<char[]> uncompress_val_data = std::make_unique<char[]>(uncompress_val_size * 1.2);
//
//     switch (_type) {
//         case COLUMN_TYPE_INTEGER: {
//             assert(uncompress_val_size == _count * sizeof(int32_t));
//             auto* int_ptr = reinterpret_cast<int32_t*>(
//                     compression::decompress_int32(reinterpret_cast<const char*>(buf), compress_val_size,
//                                                   uncompress_val_data.get(), uncompress_val_size));
//             for (int32_t i = 0; i < _count; ++i) {
//                 _column_values.emplace_back(int_ptr[i]);
//             }
//             break;
//         }
//         case COLUMN_TYPE_DOUBLE_FLOAT: {
//             assert(uncompress_val_size == _count * sizeof(double_t));
//             auto* double_ptr = reinterpret_cast<double_t*>(
//                     compression::decompress_float(reinterpret_cast<const char*>(buf), compress_val_size,
//                                                   uncompress_val_data.get(), uncompress_val_size));
//             for (int32_t i = 0; i < _count; ++i) {
//                 _column_values.emplace_back(double_ptr[i]);
//             }
//             break;
//         }
//         case COLUMN_TYPE_STRING: {
//             compression::decompress_string(reinterpret_cast<const char*>(buf), compress_val_size,
//                                            uncompress_val_data.get(), uncompress_val_size);
//             size_t str_offset = 0;
//             while (str_offset != uncompress_val_size) {
//                 int32_t str_length = *reinterpret_cast<int32_t *>(uncompress_val_data.get() + str_offset);
//                 str_offset += sizeof(int32_t);
//                 _column_values.emplace_back(uncompress_val_data.get() + str_offset, str_length);
//                 str_offset += str_length;
//             }
//             assert(str_offset == uncompress_val_size);
//             break;
//         }
//         default:
//             throw std::runtime_error("invalid column type");
//     }
//
//     buf += compress_val_size;
// }