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

#include "storage/tsm_writer.h"

#include <fstream>

namespace LindormContest {

    TsmWriter::TsmWriter(uint16_t vin_num, const Path& flush_dir_path, GlobalCompactionManagerSPtr compaction_manager)
            : _vin_num(vin_num), _compaction_manager(compaction_manager),
            _flush_dir_path(flush_dir_path), _schema(nullptr),
            _flush_nums(0), _file_nums(0), _compaction_nums(0) {
        open_flush_stream();
    }

    TsmWriter::~TsmWriter() = default;

    void TsmWriter::set_schema(SchemaSPtr schema) {
        _schema = schema;
    }

    void TsmWriter::open_flush_stream() {
        _output_file = std::make_unique<std::ofstream>();
        Path flush_file_path = _flush_dir_path / std::to_string(_file_nums);
        _output_file->open(flush_file_path, std::ios::out | std::ios::app | std::ios::binary | std::ios::ate);
        assert(_output_file->is_open() && _output_file->good());
        _flush_nums = 0;
    }

    void TsmWriter::close_flush_stream() {
        _output_file->flush();
        _output_file->close();
        _output_file = nullptr;
        if (++_file_nums % COMPACTION_FILE_NUM == 0) {
            _compaction_manager->level_compaction_async(_vin_num, _compaction_nums, _file_nums);
            _compaction_nums = _file_nums;
        }
    }

    void TsmWriter::append(const Row& row) {
        std::lock_guard<std::mutex> l(_mutex);
        if (unlikely(_output_file == nullptr)) {
            open_flush_stream();
        }
        io::write_row_to_file(*_output_file, _schema, row, false);
        if (unlikely(++_flush_nums >= FILE_FLUSH_SIZE)) {
            close_flush_stream();
        }
    }

    void TsmWriter::finalize_close_flush_stream() {
        if (_output_file == nullptr) {
            return;
        }
        _output_file->flush();
        _output_file->close();
        _output_file = nullptr;
        _compaction_manager->level_compaction_async(_vin_num, _compaction_nums, ++_file_nums);
    }

    TsmWriterManager::TsmWriterManager(const Path &root_path, GlobalCompactionManagerSPtr compaction_manager) {
        for (uint16_t vin_num = 0; vin_num < VIN_NUM_RANGE; ++vin_num) {
            Path flush_dir_path = root_path / "no-compaction" / std::to_string(vin_num);
            std::filesystem::create_directories(flush_dir_path);
            _tsm_writers[vin_num] = std::make_unique<TsmWriter>(vin_num, flush_dir_path, compaction_manager);
        }
    }

    TsmWriterManager::~TsmWriterManager() = default;

    void TsmWriterManager::set_schema(SchemaSPtr schema) {
        for (auto &tsm_writer: _tsm_writers) {
            tsm_writer->set_schema(schema);
        }
    }

    void TsmWriterManager::append(const LindormContest::Row &row) {
        uint16_t vin_num = decode_vin(row.vin);
        _tsm_writers[vin_num]->append(row);
    }

    void TsmWriterManager::finalize_close_flush_stream() {
        for (auto &tsm_writer: _tsm_writers) {
            tsm_writer->finalize_close_flush_stream();
        }
    }

}