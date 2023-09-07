//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"
#include <fstream>

namespace LindormContest {

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    TSDBEngineImpl::TSDBEngineImpl(const std::string &dataDirPath)
            : TSDBEngine(dataDirPath) {
    }

    TSDBEngineImpl::~TSDBEngineImpl() = default;

    int TSDBEngineImpl::connect() {
        _load_schema_from_file();
        return 0;
    }

    int TSDBEngineImpl::createTable(const std::string &tableName, const Schema &schema) {
        assert(SCHEMA_COLUMN_NUMS == schema.columnTypeMap.size());
        int col_id = 0;
        for (const auto &it: schema.columnTypeMap) {
            _column_names[col_id] = it.first;
            _column_types[col_id++] = it.second;
        }
        return 0;
    }

    int TSDBEngineImpl::shutdown() {
        return 0;
    }

    int TSDBEngineImpl::write(const WriteRequest &writeRequest) {
        return 0;
    }

    int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
        return 0;
    }

    int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
        return 0;
    }

    int TSDBEngineImpl::executeAggregateQuery(const TimeRangeAggregationRequest &aggregationReq,
                                              std::vector<Row> &aggregationRes) {
        return 0;
    }

    int TSDBEngineImpl::executeDownsampleQuery(const TimeRangeDownsampleRequest &downsampleReq,
                                               std::vector<Row> &downsampleRes) {
        return 0;
    }

    void TSDBEngineImpl::_save_schema_to_file() {
        std::ofstream schema_out;
        Path schema_path = _get_root_path() / "schema.txt";
        schema_out.open(schema_path, std::ios::out);
        for (int i = 0; i < SCHEMA_COLUMN_NUMS; ++i) {
            schema_out << _column_names[i] << " ";
            schema_out << (int32_t) _column_types[i] << " ";
        }
        schema_out.close();
    }

    void TSDBEngineImpl::_load_schema_from_file() {
        std::ifstream schema_fin;
        Path schema_path = _get_root_path() / "schema.txt";
        schema_fin.open(schema_path, std::ios::in);
        if (!schema_fin.is_open() || !schema_fin.good()) {
            schema_fin.close();
            INFO_LOG("schema.txt doesn't exist!")
            return;
        }

        for (int i = 0; i < SCHEMA_COLUMN_NUMS; ++i) {
            schema_fin >> _column_names[i];
            int32_t columnTypeInt;
            schema_fin >> columnTypeInt;
            _column_types[i] = (ColumnType) columnTypeInt;
        }
    }
}