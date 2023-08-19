//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"

#include <fstream>
#include <sstream>
#include <filesystem>

namespace LindormContest {

inline std::ostream& operator<<(std::ostream& os, const Vin &vin) {
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
        : TSDBEngine(dataDirPath), _column_nums(-1), _column_names(nullptr), _column_types(nullptr) {}

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
    for (const auto & it : schema.columnTypeMap) {
        _column_names[i] = it.first;
        _column_types[i] = it.second;
        i++;
    }
    return 0;
}

int TSDBEngineImpl::shutdown() {
    // Close all resources, assuming all writing and reading process has finished.
    // No mutex is fetched by assumptions.

    for (const auto& pair : _out_files) {
        pair.second->close();
        delete pair.second;
    }

    for (const auto& pair : _vin_mutex) {
        delete pair.second;
    }

    _save_schema_to_file();
    _save_latest_records_to_file();

    delete []_column_types;
    delete []_column_names;
    return 0;
}

int TSDBEngineImpl::upsert(const WriteRequest& writeRequest) {
    for (const Row& row : writeRequest.rows) {
        const Vin& vin = row.vin;
        auto& vin_mutex = _get_mutex_for_vin(vin);
        {
            std::unique_lock<std::shared_mutex> l(vin_mutex);
            std::ofstream& file_out_for_vin = _get_file_out_for_vin(vin);
            _append_row_to_file(file_out_for_vin, row, false);
            int32_t vin_num = get_vin_num(vin);
            if (row.timestamp > _latest_records[vin_num].timestamp) {
                _latest_records[vin_num] = row;
            }
        }
    }
    return 0;
}

int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
    for (const auto &vin : pReadReq.vins) {
        Row row;
        int ret = _get_latest_row(vin, pReadReq.requestedColumns, row);
        if (ret == 0) {
            pReadRes.push_back(std::move(row));
        }
    }
    return 0;
}

int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
    _get_rows_from_time_range(trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                         trReadReq.requestedColumns, trReadRes);
    return 0;
}

std::shared_mutex& TSDBEngineImpl::_get_mutex_for_vin(const Vin& vin) {
    std::shared_mutex* vin_mutex;
    {
        std::lock_guard<std::mutex> l(_global_mutex);
        const auto it = _vin_mutex.find(vin);
        if (it != _vin_mutex.cend()) {
            vin_mutex = it->second;
        } else {
            vin_mutex = new std::shared_mutex();
            _vin_mutex.emplace(vin, vin_mutex);
        }
    }
    return *vin_mutex;
}

std::ofstream& TSDBEngineImpl::_get_file_out_for_vin(const Vin &vin) {
    // Must be protected by vin's mutex.
    // Try getting from already opened set.
    {
        std::lock_guard<std::mutex> l(_global_mutex);
        auto it = _out_files.find(vin);
        if (it != _out_files.cend()) {
            auto pFileOut = it->second;
            return *pFileOut;
        }
    }
    // The first time we open the file out stream for this vin, open a new stream and put it into opened set.
    std::string vinFilePath = _get_vin_file_path(vin);
    auto *pFileOut = new std::ofstream();
    pFileOut->open(vinFilePath, std::ios::out | std::ios::app | std::ios::binary | std::ios::ate);
    if (!pFileOut->is_open() || !pFileOut->good()) {
        std::cerr << "Cannot open write stream for vin file: [" << vinFilePath << "]" << std::endl;
        delete pFileOut;
        throw std::exception();
    }
    {
        std::lock_guard<std::mutex> l(_global_mutex);
        _out_files.emplace(vin, pFileOut);
    }
    return *pFileOut;
}

int TSDBEngineImpl::_get_latest_row(const Vin& vin, const std::set<std::string>& requestedColumns, Row& result) {
    std::shared_lock<std::shared_mutex> l(_get_mutex_for_vin(vin));
    int32_t vin_num = get_vin_num(vin);
    if (vin_num == -1 || _latest_records[vin_num].timestamp == 0) {
        return -1;
    }
    Row latestRow = _latest_records[vin_num];
    result.vin = vin;
    result.timestamp = latestRow.timestamp;

    for (const auto & requestedColumn : requestedColumns) {
        result.columns.emplace(requestedColumn, latestRow.columns.at(requestedColumn));
    }
    return 0;
}

void TSDBEngineImpl::_get_rows_from_time_range(const Vin& vin, int64_t lowerInclusive, int64_t upperExclusive,
                                     const std::set<std::string>& requestedColumns,
                                     std::vector<Row>& results) {
    std::shared_lock<std::shared_mutex> l(_get_mutex_for_vin(vin));
    std::ifstream fin;
    int ret = _get_file_in_for_vin(vin, fin);
    if (ret != 0) {
        // No such vin written.
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
            for (const auto & requestedColumn : requestedColumns) {
                resultRow.columns.insert(std::make_pair(requestedColumn, nextRow.columns.at(requestedColumn)));
            }
            results.push_back(std::move(resultRow));
        }
    }

    fin.close();
}

Path TSDBEngineImpl::_get_vin_file_path(const Vin& vin) {
    std::string vinStr(vin.vin, VIN_LENGTH);
    int32_t folderNum = (int32_t) VinHasher()(vin) % 100;
    Path folder_str = _get_root_path() / std::to_string(folderNum);
    bool dirExist = std::filesystem::is_directory(folder_str);
    if (!dirExist) {
        bool created = std::filesystem::create_directories(folder_str);
        if (!created && !std::filesystem::is_directory(folder_str)) {
            std::cerr << "Cannot create directory: [" << folder_str << "]" << std::endl;
            throw std::exception();
        }
    }
    return folder_str / vinStr;
}

int TSDBEngineImpl::_read_row_from_stream(const Vin& vin, std::ifstream& fin, Row& row, bool vin_include) {
    // Must be protected by vin's mutex.
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
        std::string& cName = _column_names[cI];
        ColumnType cType = _column_types[cI];
        ColumnValue* cVal;
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
                    delete []strBuff;
                    throw std::exception();
                }
                cVal = new ColumnValue(strBuff, strLen);
                delete []strBuff;
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

void TSDBEngineImpl::_append_row_to_file(std::ofstream& fout, const Row& row, bool vin_include) {
    // Must be protected by vin's mutex
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

int TSDBEngineImpl::_get_file_in_for_vin(const Vin& vin, std::ifstream& fin) {
    // Must be protected by vin's mutex.
    Path vinFilePath = _get_vin_file_path(vin);
    std::ifstream vinFin;
    vinFin.open(vinFilePath, std::ios::in | std::ios::binary);
    if (!vinFin.is_open() || !vinFin.good()) {
        // std::cout << "Cannot get vin file input-stream for vin: [" << vin << "]. No such file" << std::endl;
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
}

void TSDBEngineImpl::_save_latest_records_to_file() {
    Path latest_records_path = _get_root_path() / "latest_records";
    std::ofstream output_file(latest_records_path, std::ios::out | std::ios::binary);
    if (!output_file.is_open()) {
        std::cerr << "Failed to open file for writing." << std::endl;
        return;
    }
//    uint32_t record_nums = _latest_records.size();
//    output_file.write((const char*) &record_nums, sizeof(uint32_t));

    for (int32_t i = 0; i < VIN_RANGE_LENGTH; ++i) {
        _append_row_to_file(output_file, _latest_records[i], true);
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
//    uint32_t record_nums;
//    input_file.read((char*) &record_nums, sizeof(record_nums));

    for (uint32_t i = 0; i < VIN_RANGE_LENGTH; ++i) {
        Row row;
        _read_row_from_stream(row.vin, input_file, row, true);
        int32_t vin_num = get_vin_num(row.vin);
        assert(vin_num == i);
        _latest_records[vin_num] = row;
    }

    input_file.close();
}

}