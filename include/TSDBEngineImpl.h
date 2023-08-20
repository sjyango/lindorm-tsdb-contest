//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#pragma once

#include "TSDBEngine.hpp"
#include "Hasher.hpp"
#include "Root.h"

#include <mutex>
#include <unordered_map>
#include <shared_mutex>

namespace LindormContest {

const size_t VIN_RANGE_LENGTH = 30000;

class TSDBEngineImpl : public TSDBEngine {
public:
    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    explicit TSDBEngineImpl(const std::string &dataDirPath);

    int connect() override;

    int createTable(const std::string &tableName, const Schema &schema) override;

    int shutdown() override;

    int upsert(const WriteRequest &wReq) override;

    int executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) override;

    int executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) override;

    ~TSDBEngineImpl() override;

private:
    Path _get_root_path() {
        return dataDirPath;
    }

    std::shared_mutex& _get_mutex_for_vin(const Vin& vin);
    std::shared_mutex& _get_mutex_for_vin_timestamp(const Vin& vin,int64_t timestamp);
    std::shared_mutex& _get_mutex_for_vin_timerange(const Vin& vin,int64_t timerange);

    // Must be protected by vin's mutex.
    // The gotten stream is shared by all caller, and should not be closed manually by caller.
    std::ofstream& _get_file_out_for_vin_timestamp(const Vin& vin, int64_t timestamp);

    // Must be protected by vin's mutex.
    // The returned ifstream is exclusive for each caller, and must be closed by caller.
    int _get_file_in_for_vin_timerange(const Vin& vin,int64_t timerange,std::ifstream& fins);

    int _get_latest_row(const Vin& vin, const std::set<std::string>& requestedColumns, Row& result);

    void _get_rows_from_time_range(const Vin& vin, int64_t lowerInclusive, int64_t upperExclusive,
                                   const std::set<std::string>& requestedColumns, std::vector<Row>& results);

    // Get the file path for this vin, there should be only one file for a vin.
    Path _get_vin_timestamp_file_path(const Vin&vin,int64_t timestamp);
    Path _get_vin_timerange_file_path(const Vin&vin,int64_t timerange);

    // Must be protected by vin's mutex.
    // Read the row from this fin. The offset of this fin should be set at the start position for the row.
    // Return 0 means success, -1 means the fin is at eof.
    int _read_row_from_stream(const Vin& vin, std::ifstream& fin, Row& row, bool vin_include);

    // Must be protected by vin's mutex.
    // Append the row to the tail of the file.
    void _append_row_to_file(std::ofstream& fout, const Row& row, bool vin_include);

    void _save_schema_to_file();

    void _load_schema_from_file();

    void _save_latest_records_to_file();

    void _load_latest_records_from_file();

    // Protect global map, such as outFiles, vinMutex defined below.
    std::mutex _global_mutex;
    // Append new written row to this file. Cache the output stream for each file. One file for a vin.
    std::unordered_map<Vin, std::ofstream*, VinHasher, VinHasher> _out_files;
    // One locker for a vin. Lock the vin's locker when reading or writing process touches the vin.
    std::unordered_map<Vin, std::shared_mutex*, VinHasher, VinHasher> _vin_mutex;

    //_vin_stamp_mutex
    std::unordered_map<std::string , std::shared_mutex*> _vin_stamp_mutex;
    // How many columns is defined in schema for the sole table.
    uint8_t _column_nums;
    // The column's type for each column.
    ColumnType* _column_types;
    // The column's name for each column.
    std::string* _column_names;
    Row _latest_records[VIN_RANGE_LENGTH];
    int64_t range = 100;
};

static int32_t get_vin_num(const Vin& vin) {
    std::string vin_str(vin.vin + 12, 5);
    try {
        int32_t vin_num = std::stoi(vin_str);
        return vin_num - 1;
    } catch (std:: exception& e) {
        return -1;
    }
}
static int64_t get_timestamp(const int64_t timestamp){
    return timestamp%1689090000000/1000;
}

}
