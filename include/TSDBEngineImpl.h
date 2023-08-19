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

    std::mutex& _get_mutex_for_vin(const Vin& vin);

    // Must be protected by vin's mutex.
    // The gotten stream is shared by all caller, and should not be closed manually by caller.
    std::ofstream& _get_file_out_for_vin(const Vin& vin);

    // Must be protected by vin's mutex.
    // The returned ifstream is exclusive for each caller, and must be closed by caller.
    int _get_file_in_for_vin(const Vin& vin, std::ifstream& fin);

    int _get_latest_row(const Vin& vin, const std::set<std::string>& requestedColumns, Row& result);

    void _get_rows_from_time_range(const Vin& vin, int64_t lowerInclusive, int64_t upperExclusive,
                                   const std::set<std::string>& requestedColumns, std::vector<Row>& results);

    // Get the file path for this vin, there should be only one file for a vin.
    Path _get_vin_file_path(const Vin&vin);

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
    std::unordered_map<Vin, std::mutex*, VinHasher, VinHasher> _vin_mutex;
    std::unordered_map<Vin, Row, VinHasher, VinHasher> _latest_records;
    std::shared_mutex _latest_mutex;
    // How many columns is defined in schema for the sole table.
    uint8_t _column_nums;
    // The column's type for each column.
    ColumnType* _column_types;
    // The column's name for each column.
    std::string* _column_names;
};

}
