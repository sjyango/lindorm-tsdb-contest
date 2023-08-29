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
#include "common/thread_pool.h"
#include "common/spinlock.h"

#include <mutex>
#include <unordered_map>
#include <shared_mutex>
#include <thread>
#include <atomic>

namespace LindormContest {

    const int32_t VIN_RANGE_LENGTH = 30000;
    const int32_t VIN_TIME_RANGE_NUM = 10;
    const int32_t VIN_TIME_RANGE_WIDTH = 3600 / VIN_TIME_RANGE_NUM;

    const uint32_t THREAD_POOL_THREAD_NUM = std::thread::hardware_concurrency() * 2;

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

        // Must be protected by vin's mutex.
        // The gotten stream is shared by all caller, and should not be closed manually by caller.
        std::ofstream &_get_file_out_for_vin_timestamp(const Vin &vin, int64_t timestamp);

        // Must be protected by vin's mutex.
        // The returned ifstream is exclusive for each caller, and must be closed by caller.
        int _get_file_in_for_vin_timestamp_range(const Vin &vin, int64_t range, std::ifstream &fins);

        int _get_latest_row(int32_t vin_num, const Vin &vin, const std::set<std::string> &requestedColumns, Row &result);

        void _get_rows_from_time_range(const Vin &vin, int64_t lower_inclusive, int64_t upper_exclusive,
                                       const std::set<std::string> &requestedColumns, std::vector<Row> &results);

        // Get the file path for this vin, there should be only one file for a vin.
        Path _get_vin_timestamp_file_path(const Vin &vin, int64_t timestamp);

        Path _get_vin_timestamp_range_file_path(const Vin &vin, int64_t range);

        // Must be protected by vin's mutex.
        // Read the row from this fin. The offset of this fin should be set at the start position for the row.
        // Return 0 means success, -1 means the fin is at eof.
        int _read_row_from_stream(const Vin &vin, std::ifstream &fin, Row &row, bool vin_include);

        // Must be protected by vin's mutex.
        // Append the row to the tail of the file.
        void _append_row_to_file(std::ofstream &fout, const Row &row, bool vin_include);

        void _save_schema_to_file();

        void _load_schema_from_file();

        void _save_latest_records_to_file();

        void _load_latest_records_from_file();

        void _visit_files_recursive(const Path& directory, std::vector<Path>& file_paths);

        void _convert_file(const Path& path, size_t* mem_usage);

        void _convert_rows_to_columns(const std::vector<Row>& rows, const Path& file_path, size_t* mem_usage);

        void _convert_columns_to_rows(std::ifstream &fin, const Vin& vin, uint16_t range, int64_t lower_inclusive, int64_t upper_exclusive,
                                      const std::set<std::string> &requestedColumns, std::vector<Row> &results);

        void _decompress_column_data(char* start, uint8_t column_index, std::vector<uint32_t>& column_compressed_offsets,
                                     std::vector<ColumnValue>& uncompress_data);

        bool _is_converted;
        uint8_t _column_nums;
        ColumnType *_column_types;
        std::string *_column_names;
        Row _latest_records[VIN_RANGE_LENGTH];
        std::unique_ptr<std::ofstream> _streams[VIN_RANGE_LENGTH * VIN_TIME_RANGE_NUM];
        std::shared_mutex _vin_mutexes[VIN_RANGE_LENGTH];
        std::shared_mutex _vin_timestamp_mutexes[VIN_RANGE_LENGTH * VIN_TIME_RANGE_NUM];
    };

    // 0 ~ 29999
    static int32_t get_vin_num(const Vin &vin) {
        int32_t vin_num = 0;
        for (int32_t i = 0; i < 5; ++i) {
            if (!std::isdigit(vin.vin[12 + i])) {
                return -1;
            }
            vin_num = vin_num * 10 + (vin.vin[12 + i] - '0');
        }
        if (vin_num >= 1 && vin_num <= 30000) {
            return vin_num - 1;
        } else {
            return -1;
        }
    }

    // 0 ~ 3599
    static uint16_t decode_timestamp(const int64_t timestamp) {
        return (timestamp / 1000) % 10000 - 1200;
    }

    static int64_t encode_timestamp(const uint16_t timestamp_num) {
        return 1689090000000 + (timestamp_num + 1200) * 1000;
    }

    static int32_t combine_vin_and_timestamp(const Vin &vin, const int64_t timestamp) {
        int32_t vin_num = get_vin_num(vin);
        assert(vin_num >= 0 && vin_num < 30000);  // 0 ~ 29999
        uint16_t timestamp_range_num = decode_timestamp(timestamp) / VIN_TIME_RANGE_WIDTH; // 0 ~ VIN_TIME_RANGE_NUM
        return vin_num + VIN_RANGE_LENGTH * timestamp_range_num;
    }

}
