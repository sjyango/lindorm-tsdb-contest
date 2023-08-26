//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include <fstream>
#include <sstream>
#include <filesystem>
#include <exception>

#include "TSDBEngineImpl.h"
#include "compression/compressor.h"

namespace LindormContest {

    inline std::ostream &operator<<(std::ostream &os, const Vin &vin) {
        std::string vinStr(vin.vin, vin.vin + VIN_LENGTH);
        os << vinStr;
        return os;
    }

    inline void swapRow(Row &lhs, Row &rhs) {
        std::swap(lhs.vin, rhs.vin);
        std::swap(lhs.timestamp, rhs.timestamp);
        lhs.columns.swap(rhs.columns);
    }

/**
 * This constructor's function signature should not be modified.
 * Our evaluation program will call this constructor.
 * The function's body can be modified.
 */
    TSDBEngineImpl::TSDBEngineImpl(const std::string &dataDirPath)
            : TSDBEngine(dataDirPath), _column_nums(0), _column_names(nullptr),
              _column_types(nullptr), _is_converted(false) {}

    TSDBEngineImpl::~TSDBEngineImpl() = default;

    int TSDBEngineImpl::connect() {
        _load_schema_from_file();
        _load_latest_records_from_file();
        return 0;
    }

    int TSDBEngineImpl::createTable(const std::string &tableName, const Schema &schema) {
        _column_nums = (uint8_t) schema.columnTypeMap.size();
        _column_names = new std::string[_column_nums];
        _column_types = new ColumnType[_column_nums];
        int i = 0;
        for (const auto &it: schema.columnTypeMap) {
            _column_names[i] = it.first;
            _column_types[i] = it.second;
            i++;
        }
        return 0;
    }

    int TSDBEngineImpl::shutdown() {
        for (auto &stream: _streams) {
            if (stream != nullptr) {
                stream->close();
            }
        }

        _save_schema_to_file();
        _save_latest_records_to_file();

        if (!_is_converted) {
            _visit_files_recursive(_get_root_path());
        }

        delete[]_column_types;
        delete[]_column_names;
        return 0;
    }

    int TSDBEngineImpl::upsert(const WriteRequest &writeRequest) {
        for (const Row &row: writeRequest.rows) {
            int32_t vin_num = get_vin_num(row.vin);
            {
                std::unique_lock<std::shared_mutex> l(_vin_mutexes[vin_num]);
                if (row.timestamp > _latest_records[vin_num].timestamp) {
                    _latest_records[vin_num] = row;
                }
            }
            {
                std::unique_lock<std::shared_mutex> l(_vin_timestamp_mutexes[combine_vin_and_timestamp(row.vin, row.timestamp)]);
                _append_row_to_file(_get_file_out_for_vin_timestamp(row.vin, row.timestamp), row, false);
            }
        }
        return 0;
    }

    int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
        for (const auto &vin: pReadReq.vins) {
            int32_t vin_num = get_vin_num(vin);
            Row row;
            int ret = _get_latest_row(vin_num, vin, pReadReq.requestedColumns, row);
            if (ret == 0) {
                pReadRes.push_back(std::move(row));
            }
        }
        return 0;
    }

    int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
        int32_t vin_num = get_vin_num(trReadReq.vin);
        if (vin_num < 0 || vin_num >= 30000) {
            INFO_LOG("executeTimeRangeQuery vin_num is out")
            return 0;
        }
        _get_rows_from_time_range(trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                                  trReadReq.requestedColumns, trReadRes);
        return 0;
    }

    std::ofstream &TSDBEngineImpl::_get_file_out_for_vin_timestamp(const Vin &vin, const int64_t timestamp) {
        int32_t index = combine_vin_and_timestamp(vin, timestamp);
        if (_streams[index] != nullptr) {
            return *_streams[index];
        }
        std::string vin_file_path = _get_vin_timestamp_file_path(vin, timestamp);
        _streams[index] = std::make_unique<std::ofstream>();
        _streams[index]->open(vin_file_path, std::ios::out | std::ios::app | std::ios::binary | std::ios::ate);
        if (!_streams[index]->is_open() || !_streams[index]->good()) {
            std::cerr << "Cannot open write stream for vin file: [" << vin_file_path << "]" << std::endl;
            throw std::exception();
        }
        return *_streams[index];
    }

    int TSDBEngineImpl::_get_latest_row(int32_t vin_num, const Vin &vin, const std::set<std::string> &requestedColumns,
                                        Row &result) {
        if (vin_num == -1 || _latest_records[vin_num].timestamp == 0) {
            INFO_LOG("executeLatestQuery vin_num is out")
            return -1;
        }
        {
            try {
                std::shared_lock<std::shared_mutex> l(_vin_mutexes[vin_num]);
                Row latestRow = _latest_records[vin_num];
                result.vin = vin;
                result.timestamp = latestRow.timestamp;
                for (const auto &requestedColumn: requestedColumns) {
                    result.columns.emplace(requestedColumn, latestRow.columns.at(requestedColumn));
                }
            } catch (std::exception &e) {
                INFO_LOG("executeLatestQuery vin_num is error")
                std::cout << "vin_num" << vin_num << std::endl;
                std::cout << "vin" << vin.vin << std::endl;
            }
        }
        return 0;
    }

    void TSDBEngineImpl::_get_rows_from_time_range(const Vin &vin, int64_t lowerInclusive, int64_t upperExclusive,
                                                   const std::set<std::string> &requestedColumns,
                                                   std::vector<Row> &results) {
        int64_t start_after = decode_timestamp(lowerInclusive);
        int64_t end_after = decode_timestamp(upperExclusive - 1000);
        if (start_after < 0 || start_after >= 3600 || end_after < 0 || end_after >= 3600) {
            return;
        }

        for (int64_t t = start_after / VIN_TIME_RANGE_WIDTH; t <= end_after / VIN_TIME_RANGE_WIDTH; t++) {
            try {
                std::shared_lock<std::shared_mutex> l(_vin_timestamp_mutexes[get_vin_num(vin) + VIN_RANGE_LENGTH * t]);
                std::ifstream fin;
                int ret = _get_file_in_for_vin_timestamp_range(vin, t, fin);
                if (ret != 0) {
                    // No such vin written.
                    std::cout << "No such vin written." << std::endl;
                    return;
                }
                while (!fin.eof()) {
                    Row nextRow;
                    ret = _read_row_from_stream(vin, fin, nextRow, false);
                    if (ret != 0) {
                        // EOF reached, no more row.
                        break;
                    }
                    if (nextRow.timestamp >= lowerInclusive && nextRow.timestamp < upperExclusive) {
                        Row resultRow;
                        resultRow.vin = vin;
                        resultRow.timestamp = nextRow.timestamp;
                        for (const auto &requestedColumn: requestedColumns) {
                            resultRow.columns.insert(
                                    std::make_pair(requestedColumn, nextRow.columns.at(requestedColumn)));
                        }
                        results.push_back(std::move(resultRow));
                    }
                }
                fin.close();
            } catch (std::exception &e) {
                INFO_LOG("execute range query is error")
                std::cout << "start_after" << start_after << std::endl;
                std::cout << "end_after" << end_after << std::endl;
                std::cout << "vin" << vin.vin << std::endl;
            }
        }
    }

    Path TSDBEngineImpl::_get_vin_timestamp_file_path(const Vin &vin, int64_t timestamp) {
        std::string vin_str(vin.vin, VIN_LENGTH);
        int64_t timestamp_num = decode_timestamp(timestamp);
        Path folder_str = _get_root_path() / std::to_string(get_vin_num(vin) % 200) / std::to_string(timestamp_num / VIN_TIME_RANGE_WIDTH);
        bool dirExist = std::filesystem::is_directory(folder_str);
        if (!dirExist) {
            bool created = std::filesystem::create_directories(folder_str);
            if (!created && !std::filesystem::is_directory(folder_str)) {
                std::cerr << "Cannot create directory: [" << folder_str << "]" << std::endl;
                throw std::exception();
            }
        }
        return folder_str / vin_str;
    }

    Path TSDBEngineImpl::_get_vin_timestamp_range_file_path(const Vin &vin, int64_t range) {
        std::string vin_str(vin.vin, VIN_LENGTH);
        Path folder_str = _get_root_path() / std::to_string(get_vin_num(vin) % 200) / std::to_string(range);
        bool dirExist = std::filesystem::is_directory(folder_str);
        if (!dirExist) {
            bool created = std::filesystem::create_directories(folder_str);
            if (!created && !std::filesystem::is_directory(folder_str)) {
                std::cerr << "Cannot create directory: [" << folder_str << "]" << std::endl;
                throw std::exception();
            }
        }
        return folder_str / vin_str;
    }


    int TSDBEngineImpl::_read_row_from_stream(const Vin &vin, std::ifstream &fin, Row &row, bool vin_include) {
        if (fin.eof()) {
            return -1;
        }

        if (vin_include) {
            fin.read((char *) vin.vin, VIN_LENGTH);
        }
        int64_t timestamp;
        fin.read((char *) &timestamp, sizeof(int64_t));
        if (fin.fail() || fin.eof()) {
            return -1;
        }
        row.vin = vin;
        row.timestamp = timestamp;

        for (int cI = 0; cI < _column_nums; ++cI) {
            std::string &cName = _column_names[cI];
            ColumnType cType = _column_types[cI];
            ColumnValue *cVal;
            switch (cType) {
                case COLUMN_TYPE_INTEGER: {
                    int32_t intVal;
                    fin.read((char *) &intVal, sizeof(int32_t));
                    if (fin.fail()) {
                        std::cerr << "Premature eof in file for vin: [" << vin
                                  << "]. The timestamp is read but no enough data attached" << std::endl;
                        throw std::exception();
                    }
                    cVal = new ColumnValue(intVal);
                    break;
                }
                case COLUMN_TYPE_DOUBLE_FLOAT: {
                    double_t doubleVal;
                    fin.read((char *) &doubleVal, sizeof(double_t));
                    if (fin.fail()) {
                        std::cerr << "Premature eof in file for vin: [" << vin
                                  << "]. The timestamp is read but no enough data attached" << std::endl;
                        throw std::exception();
                    }
                    cVal = new ColumnValue(doubleVal);
                    break;
                }
                case COLUMN_TYPE_STRING: {
                    int32_t strLen;
                    fin.read((char *) &strLen, sizeof(int32_t));
                    if (fin.fail()) {
                        std::cerr << "Premature eof in file for vin: [" << vin
                                  << "]. The timestamp is read but no enough data attached" << std::endl;
                        throw std::exception();
                    }
                    char *strBuff = new char[strLen];
                    fin.read(strBuff, strLen);
                    if (fin.fail()) {
                        std::cerr << "Premature eof in file for vin: [" << vin
                                  << "]. The timestamp is read but no enough data attached" << std::endl;
                        delete[]strBuff;
                        throw std::exception();
                    }
                    cVal = new ColumnValue(strBuff, strLen);
                    delete[]strBuff;
                    break;
                }
                default: {
                    std::cerr << "Undefined column type, this is not expected" << std::endl;
                    throw std::exception();
                }
            }
            row.columns.emplace(cName, std::move(*cVal));
            delete cVal;
        }

        return 0;
    }

    void TSDBEngineImpl::_append_row_to_file(std::ofstream &fout, const Row &row, bool vin_include) {
        if (row.columns.size() != _column_nums) {
            std::cerr << "Cannot write a non-complete row with columns' num: [" << row.columns.size() << "]. ";
            std::cerr << "There is [" << _column_nums << "] rows in total" << std::endl;
            throw std::exception();
        }

        if (vin_include) {
            fout.write((const char *) row.vin.vin, VIN_LENGTH);
        }
        fout.write((const char *) &row.timestamp, sizeof(int64_t));

        for (int i = 0; i < _column_nums; ++i) {
            std::string &cName = _column_names[i];
            const ColumnValue &cVal = row.columns.at(cName);
            int32_t rawSize = cVal.getRawDataSize();
            fout.write(cVal.columnData, rawSize);
        }
        fout.flush();
    }

    int TSDBEngineImpl::_get_file_in_for_vin_timestamp_range(const Vin &vin, int64_t range, std::ifstream &fin) {
        Path vinFilePath = _get_vin_timestamp_range_file_path(vin, range);
        std::ifstream vinFin;
        vinFin.open(vinFilePath, std::ios::in | std::ios::binary);
        if (!vinFin.is_open() || !vinFin.good()) {
            std::cout << "Cannot get vin file input-stream for vin: [" << vin << "]. No such file" << std::endl;
            return -1;
        }
        fin = std::move(vinFin);
        return 0;
    }

    void TSDBEngineImpl::_save_schema_to_file() {
        std::ofstream schema_out;
        Path schema_path = _get_root_path() / "schema";
        schema_out.open(schema_path, std::ios::out);
        schema_out << _column_nums;
        schema_out << " ";
        for (int i = 0; i < _column_nums; ++i) {
            schema_out << _column_names[i] << " ";
            schema_out << (int32_t) _column_types[i] << " ";
        }
        schema_out.close();
    }

    void TSDBEngineImpl::_load_schema_from_file() {
        std::ifstream schema_fin;
        Path schema_path = _get_root_path() / "schema";
        schema_fin.open(schema_path, std::ios::in);
        if (!schema_fin.is_open() || !schema_fin.good()) {
            std::cout << "Connect new database with empty pre-written data" << std::endl;
            schema_fin.close();
            return;
        }
        schema_fin >> _column_nums;
        if (_column_nums <= 0) {
            std::cerr << "Unexpected columns' num: [" << _column_nums << "]" << std::endl;
            schema_fin.close();
            throw std::exception();
        }
        std::cout << "Found pre-written data with columns' num: [" << _column_nums << "]" << std::endl;

        _column_types = new ColumnType[_column_nums];
        _column_names = new std::string[_column_nums];

        for (int i = 0; i < _column_nums; ++i) {
            schema_fin >> _column_names[i];
            int32_t columnTypeInt;
            schema_fin >> columnTypeInt;
            _column_types[i] = (ColumnType) columnTypeInt;
        }

        _is_converted = true;
    }

    void TSDBEngineImpl::_save_latest_records_to_file() {
        Path latest_records_path = _get_root_path() / "latest_records";
        std::ofstream output_file(latest_records_path, std::ios::out | std::ios::binary);
        if (!output_file.is_open()) {
            std::cerr << "Failed to open file for writing." << std::endl;
            return;
        }

        for (const auto &latest_record: _latest_records) {
            _append_row_to_file(output_file, latest_record, true);
        }

        output_file.flush();
        output_file.close();
    }

    void TSDBEngineImpl::_load_latest_records_from_file() {
        Path latest_records_path = _get_root_path() / "latest_records";
        if (!std::filesystem::exists(latest_records_path)) {
            return;
        }
        std::ifstream input_file(latest_records_path, std::ios::in | std::ios::binary);
        if (!input_file.is_open()) {
            INFO_LOG("latest_records file doesn't exist")
            return;
        }

        for (uint32_t i = 0; i < VIN_RANGE_LENGTH; ++i) {
            Row row;
            _read_row_from_stream(row.vin, input_file, row, true);
            int32_t vin_num = get_vin_num(row.vin);
            assert(vin_num == i);
            _latest_records[vin_num] = row;
        }

        input_file.close();
        _is_converted = true;
    }

    void TSDBEngineImpl::_visit_files_recursive(const Path &directory) {
        for (const auto &entry: std::filesystem::directory_iterator(directory)) {
            if (std::filesystem::is_directory(entry)) {
                _visit_files_recursive(entry);
            } else if (std::filesystem::is_regular_file(entry) && entry.path().filename().string().size() == 17) {
                std::ifstream fin;
                fin.open(entry.path(), std::ios::in | std::ios::binary);
                assert(fin.is_open() && fin.good());
                std::vector<Row> rows;

                while (!fin.eof()) {
                    Row nextRow;
                    if (_read_row_from_stream(nextRow.vin, fin, nextRow, false) != 0) {
                        // EOF reached, no more row.
                        break;
                    }
                    rows.emplace_back(nextRow);
                }

                fin.close();
                _convert_rows_to_columns(rows, entry.path());
            }
        }
    }

    void TSDBEngineImpl::_convert_rows_to_columns(const std::vector<Row> &rows, const Path &file_path) {
        std::ofstream fout;
        fout.open(file_path, std::ios::out | std::ios::trunc | std::ios::binary | std::ios::ate);
        assert(fout.is_open() && fout.good());
        std::vector<uint16_t> timestamps;
        std::unordered_map<std::string, std::string> columns;

        for (uint8_t i = 0; i < _column_nums; ++i) {
            std::string column_values;
            columns.emplace(_column_names[i], std::move(column_values));
        }

        for (const auto &row: rows) {
            timestamps.emplace_back(decode_timestamp(row.timestamp));
            for (const auto &col: row.columns) {
                columns[col.first].append(col.second.columnData, col.second.getRawDataSize());
            }
        }

        auto timestamp_size = static_cast<int32_t>(timestamps.size() * sizeof(uint16_t));
        fout.write((const char *) &timestamp_size, sizeof(int32_t));
        fout.write((const char *) timestamps.data(), timestamp_size);

        for (uint8_t i = 0; i < _column_nums; ++i) {
            std::string &column_values = columns[_column_names[i]];
            char *uncompress_data = column_values.data();
            auto uncompress_size = static_cast<uint32_t>(column_values.size());
            char *compress_data = new char[uncompress_size * 2];
            uint32_t compress_size;

            switch (_column_types[i]) {
                case COLUMN_TYPE_INTEGER: {
                    compress_size = LindormContest::compression::compressInteger(uncompress_data, uncompress_size,
                                                                                 compress_data);
                    break;
                }
                case COLUMN_TYPE_DOUBLE_FLOAT: {
                    compress_size = LindormContest::compression::compressFloat(uncompress_data, uncompress_size,
                                                                               compress_data);
                    break;
                }
                case COLUMN_TYPE_STRING: {
                    compress_size = LindormContest::compression::compressString(uncompress_data, uncompress_size,
                                                                                compress_data);
                    break;
                }
                default: {
                    delete[]compress_data;
                    continue;
                }
            }

            fout.write((const char *) &uncompress_size, sizeof(uint32_t));
            fout.write((const char *) &compress_size, sizeof(uint32_t));
            fout.write(compress_data, compress_size);
            delete[]compress_data;
        }

        fout.flush();
        fout.close();
    }

    void TSDBEngineImpl::_convert_columns_to_rows(std::ifstream &fin, const Vin &vin, int64_t lowerInclusive,
                                                  int64_t upperExclusive,
                                                  const std::set<std::string> &requestedColumns,
                                                  std::vector<Row> &results) {
        int32_t timestamp_size;
        fin.read((char *) &timestamp_size, sizeof(int32_t));
        std::vector<uint16_t> timestamps;
        timestamps.resize(timestamp_size / sizeof(uint16_t));
        fin.read((char *) timestamps.data(), timestamp_size);
        std::unordered_map<std::string, std::vector<ColumnValue>> columns;

        for (uint8_t i = 0; i < _column_nums; ++i) {
            uint32_t uncompress_size;
            uint32_t compress_size;
            fin.read((char *) &uncompress_size, sizeof(uint32_t));
            fin.read((char *) &compress_size, sizeof(uint32_t));
            char *compress_data = new char[compress_size];
            char *recover_data = new char[uncompress_size * 2];
            fin.read(compress_data, compress_size);
            std::vector<ColumnValue> uncompress_data;

            switch (_column_types[i]) {
                case COLUMN_TYPE_INTEGER: {
                    char *start_ptr = LindormContest::compression::decompressInteger(compress_data, compress_size,
                                                                                     recover_data, uncompress_size);
                    auto *int_ptr = reinterpret_cast<int32_t *>(start_ptr);
                    for (int j = 0; j < uncompress_size / sizeof(int32_t); ++j) {
                        uncompress_data.emplace_back(int_ptr[j]);
                    }
                    break;
                }
                case COLUMN_TYPE_DOUBLE_FLOAT: {
                    char *start_ptr = LindormContest::compression::decompressFloat(compress_data, compress_size,
                                                                                   recover_data, uncompress_size);
                    auto *double_ptr = reinterpret_cast<double_t *>(start_ptr);
                    for (int j = 0; j < uncompress_size / sizeof(double_t); ++j) {
                        uncompress_data.emplace_back(double_ptr[j]);
                    }
                    break;
                }
                case COLUMN_TYPE_STRING: {
                    LindormContest::compression::decompressString(compress_data, compress_size, recover_data,
                                                                  uncompress_size);
                    size_t offset = 0;
                    while (offset != uncompress_size) {
                        int32_t str_length = *reinterpret_cast<int32_t *>(recover_data + offset);
                        offset += sizeof(int32_t);
                        uncompress_data.emplace_back(recover_data + offset, str_length);
                        offset += str_length;
                    }
                    assert(offset == uncompress_size);
                    break;
                }
                default: {
                    delete[]compress_data;
                    delete[]recover_data;
                    continue;
                }
            }

            assert(uncompress_data.size() == timestamps.size());
            columns.emplace(_column_names[i], uncompress_data);
            delete[]compress_data;
            delete[]recover_data;
        }

        fin.close();

        for (int i = 0; i < timestamps.size(); ++i) {
            int64_t timestamp = encode_timestamp(timestamps[i]);
            if (timestamp < lowerInclusive || timestamp >= upperExclusive) {
                continue;
            }
            Row row;
            row.vin = vin;
            row.timestamp = timestamp;
            std::map<std::string, ColumnValue> sub_columns;

            for (uint8_t j = 0; j < _column_nums; ++j) {
                if (requestedColumns.find(_column_names[j]) == requestedColumns.end()) {
                    continue;
                }
                sub_columns.emplace(_column_names[j], columns[_column_names[j]][i]);
            }

            row.columns = std::move(sub_columns);
            results.emplace_back(std::move(row));
        }
    }
}